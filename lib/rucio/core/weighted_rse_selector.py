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


if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class WeightedSelector():
    """
    Representation of the RSE selector
    """

    def filter_invalid(self, rses, weight, session): 
        result_rses = []
        if weight is not None:
            for rse in rses:
                attributes = list_rse_attributes(rse_id=rse['id'], session=session)
                availability_write = True if rse.get('availability_write', True) else False
                if weight not in attributes:
                    continue  # The RSE does not have the required weight set, therefore it is ignored
                try:
                    result_rses.append({'rse_id': rse['id'],
                                      'weight': float(attributes[weight]),
                                      'mock_rse': attributes.get('mock', False),
                                      'availability_write': availability_write,
                                      'staging_area': rse['staging_area']})
                except ValueError:
                    raise InvalidRuleWeight('The RSE \'%s\' has a non-number specified for the weight \'%s\'' % (rse['rse'], weight))
        else:
            for rse in rses:
                mock_rse = has_rse_attribute(rse['id'], 'mock', session=session)
                availability_write = True if rse.get('availability_write', True) else False
                result_rses.append({'rse_id': rse['id'],
                                  'weight': 1,
                                  'mock_rse': mock_rse,
                                  'availability_write': availability_write,
                                  'staging_area': rse['staging_area']})
        
        return result_rses


    def select_rse(self, rses, copies, size, preferred_rse_ids, num_copies=0, blocklist=[], prioritize_order_over_weight=False, existing_rse_size=None):
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

        result = []
        count = copies if num_copies == 0 else num_copies

        # Remove blocklisted rses
        if blocklist:
            rses = [rse for rse in rses if rse['rse_id'] not in blocklist]
        if len(rses) < count:
            raise InsufficientTargetRSEs('There are not enough target RSEs to fulfil the request at this time.')

        # Remove rses which do not have enough space, accounting for the files already at each rse
        if existing_rse_size is None:
            existing_rse_size = {}
        rses = [rse for rse in rses if rse['space_left'] >= size - existing_rse_size.get(rse['rse_id'], 0)]
        if len(rses) < count:
            raise RSEOverQuota('There is insufficient space on any of the target RSE\'s to fullfill the operation.')

        # Remove rses which do not have enough local quota
        rses = [rse for rse in rses if rse['quota_left'] > size]
        if len(rses) < count:
            raise InsufficientAccountLimit('There is insufficient quota on any of the target RSE\'s to fullfill the operation.')

        # Remove rses which do not have enough global quota
        rses_with_enough_quota = []
        for rse in rses:
            enough_global_quota = True
            for rse_expression in rse.get('global_quota_left', []):
                if rse['global_quota_left'][rse_expression] < size:
                    enough_global_quota = False
                    break
            if enough_global_quota:
                rses_with_enough_quota.append(rse)
        rses = rses_with_enough_quota
        if len(rses) < count:
            raise InsufficientAccountLimit('There is insufficient quota on any of the target RSE\'s to fullfill the operation.')

        for copy in range(count):
            # Remove rses already in the result set
            rses = [rse for rse in rses if rse['rse_id'] not in [item[0] for item in result]]
            rses_dict = {}
            for rse in rses:
                rses_dict[rse['rse_id']] = rse
            # Prioritize the preffered rses
            preferred_rses = [rses_dict[rse_id] for rse_id in preferred_rse_ids if rse_id in rses_dict]
            if prioritize_order_over_weight and preferred_rses:
                rse = (preferred_rses[0]['rse_id'], preferred_rses[0]['staging_area'], preferred_rses[0]['availability_write'])
            elif preferred_rses:
                rse = self.__choose_rse(preferred_rses)
            else:
                rse = self.__choose_rse(rses)
            result.append(rse)
            self.__update_quota(rses, rse, size)
        return result

    def __choose_rse(self, rses):
        """
        Choose an RSE based on weighting.

        :param rses:  The rses to be considered for the choose.
        :return:      The (rse_id, staging_area) tuple of the chosen RSE.
        """

        shuffle(rses)
        pick = uniform(0, sum([rse['weight'] for rse in rses]))
        weight = 0
        for rse in rses:
            weight += rse['weight']
            if pick <= weight:
                return (rse['rse_id'], rse['staging_area'], rse['availability_write'])

    def __update_quota(self, rses, rse, size):
        """
        Update the internal quota value.

        :param rse:      RSE tuple to update.
        :param size:     Size to substract.
        """

        for element in rses:
            if element['rse_id'] == rse[0]:
                element['quota_left'] -= size
                for rse_expression in element.get('global_quota_left', []):
                    element['global_quota_left'][rse_expression] -= size
                return