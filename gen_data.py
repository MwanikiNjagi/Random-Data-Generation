import numpy as np
import pandas  as pd

#reads file
lookup_df = pd.read_csv("lookup.csv")

#argument declarations for func
chunks = 100,000 #Number of files to be generated
chunksize = 10,000,000 #number of rows per file
std = 10 #standard deviation

#func that generates data from lookup
def generate_chunk(chunks, chunksize, std, lookup_df):

    """Generate some sample data based on the lookup table."""

    rng = np.random.default_rng(chunks)  # Determinisitic data generation
    df = pd.DataFrame(
        {
            # Choose a random station from the lookup table for each row in our output
            "station": rng.integers(0, len(lookup_df) - 1, int(chunksize)),
            # Generate a normal distibution around zero for each row in our output
            # Because the std is the same for every station we can adjust the mean for each row afterwards
            "measure": rng.normal(0, std, int(chunksize)),
        }
    )

    # Offset each measurement by the station's mean value
    df.measure += df.station.map(lookup_df.mean_temp)
    # Round the temprature to one decimal place
    df.measure = df.measure.round(decimals=1)
    # Convert the station index to the station name
    df.station = df.station.map(lookup_df.station)
    #splits the data_frame into our desired number of chunks 
    chunks = np.array_split(df, chunks)

    for count, chunk in enumerate(chunks):
        chunk.to_csv(f"measurement-{count}.csv", index = False)


#calling function
generate_chunk(chunks=chunks, chunksize=chunksize, std=std, lookup_df=lookup_df)