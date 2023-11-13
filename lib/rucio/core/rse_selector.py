# -*- coding: utf-8 -*-
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from random import uniform, shuffle
from typing import TYPE_CHECKING

from rucio.common.exception import InsufficientAccountLimit, InsufficientTargetRSEs, InvalidRuleWeight, RSEOverQuota
from rucio.core.account import has_account_attribute, get_usage, get_all_rse_usages_per_account
from rucio.core.account_limit import get_local_account_limit, get_global_account_limits
from rucio.core.rse import list_rse_attributes, has_rse_attribute, get_rse_limits
from rucio.core.rse_counter import get_counter as get_rse_counter
from rucio.core.rse_expression_parser import parse_expression
from rucio.db.sqla.session import read_session
from rucio.core.weighted_rse_selector import WeightedSelector
from rucio.core.cloud_rse_selector import CloudSelector

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class RSESelector():
    """
    Representation of the RSE selector
    """

    @read_session
    def __init__(self, account, rses, weight, copies, ignore_account_limit=False, *, session: "Session"):
        """
        Initialize the RSE Selector.

        :param account:               Account owning the rule.
        :param rses:                  List of rse dictionaries.
        :param weight:                Weighting to use.
        :param copies:                Number of copies to create.
        :param ignore_account_limit:  Flag if the quota should be ignored.
        :param session:               DB Session in use.
        :raises:                      InvalidRuleWeight, InsufficientAccountLimit, InsufficientTargetRSEs
        """
        self.account = account
        self.rses = []  # [{'rse_id':, 'weight':, 'staging_area'}]
        self.copies = copies

        if weight == "cloud":
            self.selector = CloudSelector()
        else: 
            self.selector = WeightedSelector()

        self.rses = self.selector.filter_invalid(rses, weight, session)
        if len(self.rses) < self.copies:
            raise InsufficientTargetRSEs('Target RSE set not sufficient for number of copies. (%s copies requested, RSE set size %s)' % (self.copies, len(self.rses)))

        self.rses = self.filter_quota(account, session, ignore_account_limit)
        if len(self.rses) < self.copies:
            raise InsufficientAccountLimit('There is insufficient quota on any of the target RSE\'s to fullfill the operation.')

    def filter_quota(self, account, session, ignore_account_limit):
        rses_with_enough_quota = []
        if has_account_attribute(account=account, key='admin', session=session) or ignore_account_limit:
            for rse in self.rses:
                rse['quota_left'] = float('inf')
                rse['space_left'] = float('inf')
                rses_with_enough_quota.append(rse)
        else:
            global_quota_limit = get_global_account_limits(account=account, session=session)
            all_rse_usages = {usage['rse_id']: usage['bytes'] for usage in get_all_rse_usages_per_account(account=account, session=session)}
            for rse in self.rses:
                if rse['mock_rse']:
                    rse['quota_left'] = float('inf')
                    rse['space_left'] = float('inf')
                    rses_with_enough_quota.append(rse)
                else:
                    # check local quota
                    local_quota_left = None
                    quota_limit = get_local_account_limit(account=account, rse_id=rse['rse_id'], session=session)
                    if quota_limit is None:
                        local_quota_left = 0
                    else:
                        local_quota_left = quota_limit - get_usage(rse_id=rse['rse_id'], account=account, session=session)['bytes']

                    # check global quota
                    rse['global_quota_left'] = {}
                    all_global_quota_enough = True
                    for rse_expression, limit in global_quota_limit.items():
                        if rse['rse_id'] in limit['resolved_rse_ids']:
                            quota_limit = limit['limit']
                            global_quota_left = None
                            if quota_limit is None:
                                global_quota_left = 0
                            else:
                                rse_expression_usage = 0
                                for rse_id in limit['resolved_rse_ids']:
                                    rse_expression_usage += all_rse_usages.get(rse_id, 0)
                                global_quota_left = quota_limit - rse_expression_usage
                            if global_quota_left <= 0:
                                all_global_quota_enough = False
                                break
                            else:
                                rse['global_quota_left'][rse_expression] = global_quota_left
                    if local_quota_left > 0 and all_global_quota_enough:
                        rse['quota_left'] = local_quota_left
                        space_limit = get_rse_limits(name='MaxSpaceAvailable', rse_id=rse['rse_id'], session=session).get('MaxSpaceAvailable')
                        if space_limit is None or space_limit < 0:
                            rse['space_left'] = float('inf')
                        else:
                            rse['space_left'] = space_limit - get_rse_counter(rse_id=rse['rse_id'], session=session)['bytes']
                        rses_with_enough_quota.append(rse)
        
        return rses_with_enough_quota

    def get_rse_dictionary(self):
        """
        Return the current dictionary of potential RSEs stored in the RSE selector

        :returns:  List of RSE dictionaries
        """
        rse_dict = {}
        for rse in self.rses:
            rse_dict[rse['rse_id']] = rse
        return rse_dict


            
    def select_rse(self, size, preferred_rse_ids, copies=0, blocklist=[], prioritize_order_over_weight=False, existing_rse_size=None):
        """
        Select n RSEs to replicate data to.

        :param size:                         Size of the block being replicated.
        :param preferred_rse_ids:            Ordered list of preferred rses. (If possible replicate to them)
        :param copies:                       Select this amount of copies, if 0 use the pre-defined rule value.
        :param blocklist:                    List of blocked rses. (Do not put replicas on these sites)
        :param prioritze_order_over_weight:  Prioritize the order of the preferred_rse_ids list over the picking done by weight.
        :existing_rse_size:                  Dictionary of size of files already present at each rse
        :returns:                            List of (RSE_id, staging_area, availability_write) tuples.
        :raises:                             InsufficientAccountLimit, InsufficientTargetRSEs
        """
        return self.selector.select_rse(self.rses, self.copies, size, preferred_rse_ids, copies=0, blocklist=[], prioritize_order_over_weight=False, existing_rse_size=None)

@read_session
def resolve_rse_expression(rse_expression, account, weight=None, copies=1, ignore_account_limit=False, size=0, preferred_rses=[], blocklist=[], prioritize_order_over_weight=False, existing_rse_size=None, *, session: "Session"):
    """
    Resolve a potentially complex RSE expression into `copies` single-RSE expressions. Uses `parse_expression()`
    to decompose the expression, then `RSESelector.select_rse()` to pick the target RSEs.
    """

    rses = parse_expression(rse_expression, filter_={'vo': account.vo}, session=session)

    rse_to_id = dict((rse_dict['rse'], rse_dict['id']) for rse_dict in rses)
    id_to_rse = dict((rse_dict['id'], rse_dict['rse']) for rse_dict in rses)

    selector = RSESelector(account=account,
                           rses=rses,
                           weight=weight,
                           copies=copies,
                           ignore_account_limit=ignore_account_limit,
                           session=session)

    preferred_rse_ids = [rse_to_id[rse] for rse in preferred_rses if rse in rse_to_id]

    preferred_unmatched = list(set(preferred_rses) - set(rse_dict['rse'] for rse_dict in rses))

    selection_result = selector.select_rse(size=size,
                                           preferred_rse_ids=preferred_rse_ids,
                                           blocklist=blocklist,
                                           prioritize_order_over_weight=prioritize_order_over_weight,
                                           existing_rse_size=existing_rse_size)

    return [id_to_rse[rse_id] for rse_id, _, _ in selection_result], preferred_unmatched