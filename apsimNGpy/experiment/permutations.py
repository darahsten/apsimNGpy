from itertools import product
factors = ['crop_code', 'Nitrogen', "ResidueFractions", "ApplicationType", "Tillage", "Clusters"]

def create_permutations(factors:list, factor_names: list):
        """_summary_

        The create_permutations function is designed to generate a dictionary of permutations from
        a list of factors, with each permutation indexed by an enumeration and the factor values labeled
        by corresponding names provided in factor_names. This documentation outlines its purpose, parameters, return value, and raises conditions.

        The Purpose of
        The function aims to create a comprehensive dictionary of all possible combinations (permutations) generated
         from the provided lists of factors. Each permutation is paired with an index, serving as a unique identifier, and
         the factors within each permutation are labeled according to the corresponding names given in factor_names.

        Parameters
        factors (list of lists): A list where each element is a list representing a factor. Each sublist contains
        the possible values that the factor can take.
        factor_names (list of str): A list of strings where each string represents the name of a factor. The order
        of names should match the order of factor lists in factors.
        Returns
        A dictionary where each key is an integer index (starting from 0) corresponding to a unique permutation.
        The value for each key is another dictionary where each key-value pair corresponds to a factor name (from factor_names)
        and its value in the permutation.
        Raises
        ValueError: If the length of factor_names does not match the length of factors, indicating a mismatch between the number of
         provided factor names and the number of factors. The error message is: "Unacceptable - factor names should have the same length as the factor list."
        """
    
        if len(factor_names) != len(factor_names):
            raise ValueError("Un acceptable factor names should have the same length as the factor list ")
        permutations = list(product(*factors))
        
        # attach simulation_id
        return  {i:dict(zip(factor_names, m)) for i, m in enumerate(permutations)}
class GenerateCombinations:
    def __init__(self, management_factors):
          self.mgt = management_factors
    @property
    def management_combination(self):
        return  create_permutations([n.variables for n in self.mgt], [i.parameter for i in self.mgt])
    @property
    def organise_scrips(self):
        return [i.get_script_manager for i in self.mgt ]

def mgt_updater(simId, ap, old_list):
   upd = ap[simId]
   upd = list(ap[simId].keys())
   for new, old in zip(upd, old_list):
       print(old)
       if new in old.keys():
          old[new] = ap[simId][new]

   return old_list


if __name__ == "__main__":
       def generateFactors():
          mn =  {'Name': "Simple Rotation", 'Parameters': ["Crops", "Cropb"], 'Variables':['Maize, Soybean', 'Maize, Wheat']}
          org ={i: m for i in mn.get('Parameters') for m in mn.get('Variables')}
          org['Name']= mn.get('Name')
          return org
       print(generateFactors())
       from variable import  DiscreteVariable, BoundedVariable
       N = DiscreteVariable(options=[100, 200], place_holder_name='Nitrogen', parameter='Amount', manager='NitrogenManger')
       Dep = BoundedVariable(bounds = (0, 350), place_holder_name='depth', manager='Tillage', parameter='Depth')
       comb = GenerateCombinations([N, Dep])
       ap= comb.management_combination







