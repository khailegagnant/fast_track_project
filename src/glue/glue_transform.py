import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, concat_ws, explode_outer, to_date, to_timestamp
# ==========================================
# 1. PARSING FUNCTIONS
# ==========================================
def parse_circuits(df):
    exploded_df = df.select(explode_outer(col("MRData.CircuitTable.Circuits")).alias("circuit"))
    return exploded_df.select(
        col("circuit.circuitId").alias("circuit_id"),
        col("circuit.circuitName").alias("circuit_name"),
        col("circuit.Location.locality").alias("locality"),
        col("circuit.Location.country").alias("country"),
        col("circuit.Location.lat").cast("float").alias("latitude"),
        col("circuit.Location.long").cast("float").alias("longitude")
    )

# Add parse_drivers, parse_results, etc. here as we build them
def parse_constructors(df):
    """
    Parses the Bronze constructors JSON payload.
    Flattens the 'Constructors' array into individual rows.
    """
    exploded_df = df.select(
        explode_outer(col("MRData.ConstructorTable.Constructors")).alias("constructor")
    )
    
    flattened_df = exploded_df.select(
        col("constructor.constructorId").alias("constructor_id"),
        col("constructor.name").alias("name"),
        col("constructor.nationality").alias("nationality")
    )
    
    return flattened_df



def parse_drivers(df):
    """
    Parses the Bronze drivers JSON payload.
    Flattens the 'Drivers' array and extracts driver profile fields.
    """
    exploded_df = df.select(
        explode_outer(col("MRData.DriverTable.Drivers")).alias("driver")
    )
    
    flattened_df = exploded_df.select(
        col("driver.driverId").alias("driver_id"),
        col("driver.permanentNumber").cast("int").alias("permanent_number"),
        col("driver.code").alias("driver_code"),
        col("driver.givenName").alias("given_name"),
        col("driver.familyName").alias("family_name"),
        col("driver.dateOfBirth").cast("date").alias("date_of_birth"),
        col("driver.nationality").alias("nationality")
    )
    
    return flattened_df


def parse_races(df):
    """
    Parses the Bronze races JSON payload.
    Flattens the 'Races' array and extracts race calendar metadata.
    Combines date and time into a unified UTC timestamp.
    """
    exploded_df = df.select(
        explode_outer(col("MRData.RaceTable.Races")).alias("race")
    )
    
    flattened_df = exploded_df.select(
        col("race.season").cast("int").alias("season"),
        col("race.round").cast("int").alias("round"),
        col("race.raceName").alias("race_name"),
        col("race.Circuit.circuitId").alias("circuit_id"),
        to_date(col("race.date")).alias("race_date"),
        col("race.time").alias("race_time"),
        to_timestamp(
            concat_ws(" ", col("race.date"), col("race.time")), 
            "yyyy-MM-dd HH:mm:ss'Z'"
        ).alias("race_timestamp_utc"),
        col("race.url").alias("race_url")
    )
    
    return flattened_df


def parse_qualifying(df):
    """
    Parses the Bronze qualifying JSON payload.
    This requires a double-explode: first flattening the 'Races' array, 
    then flattening the 'QualifyingResults' array inside each race.
    """
    # 1. Explode the Races array (One row per race)
    races_df = df.select(
        explode_outer(col("MRData.RaceTable.Races")).alias("race")
    )
    
    # 2. Explode the QualifyingResults array (One row per driver per race)
    qual_df = races_df.select(
        col("race.season").cast("int").alias("season"),
        col("race.round").cast("int").alias("round"),
        explode_outer(col("race.QualifyingResults")).alias("qual_result")
    )
    
    # 3. Extract the final columns for the Silver layer
    flattened_df = qual_df.select(
        col("season"),
        col("round"),
        col("qual_result.Driver.driverId").alias("driver_id"),
        col("qual_result.Constructor.constructorId").alias("constructor_id"),
        col("qual_result.position").cast("int").alias("position"),
        col("qual_result.Q1").alias("q1_time_str"),
        col("qual_result.Q2").alias("q2_time_str"),
        col("qual_result.Q3").alias("q3_time_str")
    )
    
    return flattened_df


def parse_results(df):
    """
    Parses the Bronze race results JSON payload.
    Requires a double-explode: first flattening the 'Races' array, 
    then flattening the 'Results' array inside each race.
    """
    # 1. Explode the Races array (One row per race)
    races_df = df.select(
        explode_outer(col("MRData.RaceTable.Races")).alias("race")
    )
    
    # 2. Explode the Results array (One row per driver per race)
    results_df = races_df.select(
        col("race.season").cast("int").alias("season"),
        col("race.round").cast("int").alias("round"),
        explode_outer(col("race.Results")).alias("result")
    )
    
    # 3. Extract the final columns for the Silver layer
    flattened_df = results_df.select(
        col("season"),
        col("round"),
        col("result.Driver.driverId").alias("driver_id"),
        col("result.Constructor.constructorId").alias("constructor_id"),
        col("result.grid").cast("int").alias("grid_position"),
        col("result.position").cast("int").alias("finish_position"),
        col("result.positionText").alias("position_text"),
        col("result.points").cast("float").alias("points_scored"),
        col("result.laps").cast("int").alias("laps_completed"),
        col("result.status").alias("status"),
        col("result.Time.millis").cast("int").alias("race_time_millis"),
        col("result.FastestLap.rank").cast("int").alias("fastest_lap_rank"),
        col("result.FastestLap.Time.time").alias("fastest_lap_time_str")
    )
    
    return flattened_df
# ==========================================
# 2. ROUTING DICTIONARY
# ==========================================
PARSERS = {
    "circuits": parse_circuits,
    "constructors": parse_constructors,
    "drivers": parse_drivers,
    "races": parse_races,
    "qualifying": parse_qualifying,
    "results": parse_results,
    # "driver_standings": parse_driver_standings,
    # "constructor_standings": parse_constructor_standings
}

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def main():
    # Capture arguments injected by Terraform / Airflow
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'bronze_bucket', 
        'silver_bucket',
        'run_season' # e.g., '2023' or 'global'
    ])

    # Initialize Glue Context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    bronze_bucket = args['bronze_bucket']
    silver_bucket = args['silver_bucket']
    season = args['run_season']

    # Process each registered endpoint
    for endpoint, parser_func in PARSERS.items():
        print(f"Starting transformation for {endpoint} (Season: {season})")
        
        # Read from Bronze
        input_path = f"s3://{bronze_bucket}/bronze/f1/endpoint={endpoint}/season={season}/"
        try:
            raw_df = spark.read.option("multiline", "true").json(input_path)
            
            # Apply the specific flattening logic
            flattened_df = parser_func(raw_df)
            
            # Write to Silver (Parquet with Snappy compression)
            output_path = f"s3://{silver_bucket}/silver/f1/{endpoint}/season={season}/"
            flattened_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("compression", "snappy") \
                .save(output_path)
                
            print(f"Successfully wrote {endpoint} to Silver layer.")
        except Exception as e:
            print(f"Failed to process {endpoint} at {input_path}: {str(e)}")

    job.commit()

if __name__ == "__main__":
    main()