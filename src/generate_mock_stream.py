import os
import time
import random
import numpy as np
import pandas as pd
from datetime import datetime

# Create directory for streaming files if it doesn't exist
os.makedirs("../data/stream_source", exist_ok=True)

# Number of transactions to generate
num_transactions = 100
batch_size = 10

# Function to generate a random transaction (similar to the creditcard dataset format)
def generate_random_transaction():
    # Generate random values similar to the V1-V28 PCA components
    features = [random.uniform(-3, 3) for _ in range(28)]
    
    # Generate random time and amount
    transaction_time = random.uniform(0, 172800)  # 0-48 hours in seconds
    amount = random.uniform(1, 2000)
    
    # Most transactions are not fraudulent (only about 0.2% are)
    transaction_class = 0
    if random.random() < 0.002:
        transaction_class = 1
    
    # Return as a dictionary - only raw features that match the original data format
    return {
        'Time': transaction_time,
        **{f'V{i+1}': val for i, val in enumerate(features)},
        'Amount': amount,
        'Class': transaction_class
    }

print("Starting to generate streaming data...")
for batch in range(num_transactions // batch_size):
    # Generate a batch of transactions
    transactions = [generate_random_transaction() for _ in range(batch_size)]
    
    # Create DataFrame
    df = pd.DataFrame(transactions)
    
    # Save as CSV with timestamp to ensure uniqueness
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
    output_file = f"../data/stream_source/transactions_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    
    print(f"Generated batch {batch+1}/{num_transactions//batch_size} - {output_file}")
    
    # Wait a bit before generating the next batch
    time.sleep(1)

print("Finished generating streaming data.") 