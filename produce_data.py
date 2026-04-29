# Import any important libraries
import pandas as pd
import time
import os

# Make sure that `power_streaming_data.csv` is in the same place as this `.py` script
data_source = "power_streaming_data.csv"
df = pd.read_csv(data_source)

# Define the output folder, this must be the same as our folder in the notebook (i.e. `streaming_folder`)
output_folder = "streaming_folder"

# Create the folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
    
# Start the data generation loop
print("Starting data stream simulation...")

# Run for 20 iterations as requested
for i in range(20):
    # Randomly sample five rows from the dataframe 
    sample = df.sample(n=5)
    
    # Generate a unique filename for each batch
    file_path = os.path.join(output_folder, f"batch_{i}.csv")
    
    # Output the sample to a CSV file 
    # index=False ensures we don't write row indices 
    # header=True is handled by the Spark stream configuration
    sample.to_csv(file_path, index=False, header=True)
    
    print(f"Iteration {i+1}: Wrote 5 rows to {file_path}")
    
    # Pause for 10 seconds between iterations 
    time.sleep(30)
    
# Mark the end of the data stream simulation
print("Data stream simulation complete.")

