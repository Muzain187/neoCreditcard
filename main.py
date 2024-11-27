"""
Each time when you want to load the particular node here in the main function
import that file and call the function and also load the particular data file
"""


import nodes.user_load_nodes as user_load_nodes 
from nodes import credit_card_load_nodes 
from nodes import location_load_nodes
from nodes import merchants_load_node
from nodes import merchant_category_load_nodes
from nodes import trasnactions_load_node

from edges import userToCreditcard
from edges import creditcardToTransaction
 
def main():
    """
    Loading Nodes
    """
    # print("Adding User node")
    # user_load_nodes.load_nodes(r"D:\neo4jCreditcard\datasets\User.jsonl")

    # print("Adding CreditCard node")
    # credit_card_load_nodes.load_nodes(r"D:\neo4jCreditcard\datasets\Creditcard.jsonl")

    # location_load_nodes.load_nodes(r"datasets/LocationData.jsonl")
    # location_load_nodes.load_nodes(r"datasets/LocationDataforUser.jsonl")


    # merchants_load_node.load_nodes(r"datasets/Merchant.jsonl")
    
    # merchant_category_load_nodes.load_nodes(r"datasets/Merchant_Category.jsonl")

    # trasnactions_load_node.load_nodes(r"datasets/TransactionData.jsonl")

    """
    Loading Edges
    """
    # userToCreditcard.load_relationships(r"datasets/User_TO_CreditCard.jsonl")

    creditcardToTransaction.load_relationships(r"datasets/creditcardToTransaction.jsonl")

 
    # Add more calls for other vertex types as needed
    print("All nodes and edges loaded successfully!")
 
if __name__ == "__main__":
    main()