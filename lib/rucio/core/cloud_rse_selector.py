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

READ_FREQUENCY = 1.0

def costWrapper(file_size):
    def getRSECost(rse):
        storage_cost = file_size * rse['storage_cost_per_gb']
        data_transfer_cost = READ_FREQUENCY * file_size * rse['data_transfer_cost_per_gb'] 
        data_access_cost = READ_FREQUENCY * rse['data_access_cost_per_gb'] 
        return storage_cost + data_transfer_cost + data_access_cost
    
    return getRSECost

# def calculate_minimal_cost(Gs, file_size):
#     """
#     Calculate the minimal cost for storing and reading data from a set of clouds.
#     Assuming cost is a function of file size, read frequency, and individual cloud costs.
#     """
#     total_cost = 0
#     for cloud in Gs:
#         storage_cost = file_size * cloud['storage_cost_per_gb']
#         data_transfer_cost = READ_FREQUENCY * file_size * cloud['data_transfer_cost_per_gb'] 
#         data_access_cost = READ_FREQUENCY * cloud['data_access_cost_per_gb'] 
#         total_cost += storage_cost + data_transfer_cost + data_access_cost
#     return total_cost

# def sort_clouds_by_price(rses, file_size):
#     rses.sort(key=costWrapper(file_size))
#     return rses

def calculate_weights(rses, file_size):
    
    costFunction = costWrapper(file_size)

    for rse in rses:
        rse['weight'] = 1.0 / costFunction(rse)


class CloudSelector():
    """
    Representation of the RSE selector
    """

    def filter_invalid(self, rses, weight, session): 
        result_rses = []
        
        for rse in rses:
            attributes = list_rse_attributes(rse_id=rse['id'], session=session)
            availability_write = True if rse.get('availability_write', True) else False
            if weight not in attributes:
                continue  # The RSE is not hosted on the cloud, so it is ignored
            
            result_rses.append({'rse_id': rse['id'],
                                'mock_rse': attributes.get('mock', False),
                                'availability_write': availability_write,
                                'staging_area': rse['staging_area'], 
                                'storage_cost_per_gb': rse['storage_cost_per_gb'], 
                                'data_transfer_cost_per_gb': rse['data_transfer_cost_per_gb'], 
                                'data_access_cost_per_gb': rse['data_access_cost_per_gb']})

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
        
        calculate_weights(rses, size)

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
    
    
#     import numpy as np

# def vAsVector(v):
#     # Convert location object 'v' to a vector representation
#     # This function needs to be defined based on how 'v' is represented
#     return np.array(v)

# def normalize(v):
#     # Normalize a vector 'v'
#     return v / np.linalg.norm(v)

# def computePs(HPs):
#     # Compute set of points based on the hyperplanes
#     # This function needs a proper implementation based on the algorithm's requirements
#     return []

# def algorithm_CMO(V, Fk):
#     # V: list of location objects
#     # Fk: dictionary where keys are CDN indices and values are feasibility sets

#     # Step 1: Identify hyperplanes
#     HPs = []
#     for v in V:
#         vVec = vAsVector(v)
#         for k in Fk:
#             for j in Fk:
#                 if k != j and v in Fk[k] and v in Fk[j]:
#                     ek = np.zeros(len(Fk))
#                     ej = np.zeros(len(Fk))
#                     ek[k] = 1
#                     ej[j] = 1
#                     hpCandidate = normalize(np.cross(vVec, ek - ej))
#                     if not any(np.array_equal(hpCandidate, hp) for hp in HPs):
#                         HPs.append(hpCandidate)

#     # Step 2: Compute interior points from hyperplanes
#     Ps = computePs(HPs)

#     # Step 3: Evaluate extremal assignments identified by Ps
#     optAs = None

#     for P in Ps:
#         ψ = {}
#         for v in V:
#             optOuter = float('inf')
#             for k in Fk:
#                 if v in Fk[k]:
#                     value = np.dot(P, np.outer(v, ek[k]))
#                     if value < optOuter:
#                         ψ[v] = k
#                         optOuter = value

#         if optAs is None or some_comparison_function(ψ, optAs):
#             optAs = ψ

#     return optAs

# # Example usage
# V = [...]  # Your list of location objects
# Fk = {...}  # Your dictionary of feasibility sets for each CDN
# optAs = algorithm_CMO(V, Fk)
