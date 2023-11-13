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
                                'staging_area': rse['staging_area']})
    
        return result_rses

    def select_rse(self, size, preferred_rse_ids, copies=0, blocklist=[], prioritize_order_over_weight=False, existing_rse_size=None):
        return
    
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
