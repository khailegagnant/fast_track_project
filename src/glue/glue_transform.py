import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, concat_ws, explode_outer, to_date, to_timestamp

# ==========================================
# LOGGING SETUP
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("glue_silver_transformer")

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

def parse_constructors(df):
    exploded_df = df.select(explode_outer(col("MRData.ConstructorTable.Constructors")).alias("constructor"))
    return exploded_df.select(
        col("constructor.constructorId").alias("constructor_id"),
        col("constructor.name").alias("name"),
        col("constructor.nationality").alias("nationality")
    )

def parse_drivers(df):
    exploded_df = df.select(explode_outer(col("MRData.DriverTable.Drivers")).alias("driver"))
    return exploded_df.select(
        col("driver.driverId").alias("driver_id"),
        col("driver.permanentNumber").cast("int").alias("permanent_number"),
        col("driver.code").alias("driver_code"),
        col("driver.givenName").alias("given_name"),
        col("driver.familyName").alias("family_name"),
        col("driver.dateOfBirth").cast("date").alias("date_of_birth"),
        col("driver.nationality").alias("nationality")
    )

def parse_races(df):
    exploded_df = df.select(explode_outer(col("MRData.RaceTable.Races")).alias("race"))
    return exploded_df.select(
        col("race.season").cast("int").alias("season"),
        col("race.round").cast("int").alias("round"),
        col("race.raceName").alias("race_name"),
        col("race.Circuit.circuitId").alias("circuit_id"),
        to_date(col("race.date")).alias("race_date"),
        col("race.time").alias("race_time"),
        to_timestamp(concat_ws(" ", col("race.date"), col("race.time")), "yyyy-MM-dd HH:mm:ss'Z'").alias("race_timestamp_utc"),
        col("race.url").alias("race_url")
    )

def parse_qualifying(df):
    races_df = df.select(explode_outer(col("MRData.RaceTable.Races")).alias("race"))
    qual_df = races_df.select(
        col("race.season").cast("int").alias("season"),
        col("race.round").cast("int").alias("round"),
        explode_outer(col("race.QualifyingResults")).alias("qual_result")
    )
    return qual_df.select(
        col("season"),
        col("round"),
        col("qual_result.Driver.driverId").alias("driver_id"),
        col("qual_result.Constructor.constructorId").alias("constructor_id"),
        col("qual_result.position").cast("int").alias("position"),
        col("qual_result.Q1").alias("q1_time_str"),
        col("qual_result.Q2").alias("q2_time_str"),
        col("qual_result.Q3").alias("q3_time_str")
    )

def parse_results(df):
    races_df = df.select(explode_outer(col("MRData.RaceTable.Races")).alias("race"))
    results_df = races_df.select(
        col("race.season").cast("int").alias("season"),
        col("race.round").cast("int").alias("round"),
        explode_outer(col("race.Results")).alias("result")
    )
    return results_df.select(
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

# ==========================================
# 2. ROUTING DICTIONARIES
# ==========================================
# Split parsers to ensure we don't look for seasonal data in the global run (and vice versa)
GLOBAL_PARSERS = {
    "circuits": parse_circuits
}

SEASON_PARSERS = {
    "constructors": parse_constructors,
    "drivers": parse_drivers,
    "races": parse_races,
    "qualifying": parse_qualifying,
    "results": parse_results
}

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def main():
    # 1. Catch the variables passed from your .env via Airflow
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'bronze_bucket', 
        'silver_bucket',
        'run_season',
        'bronze_prefix',
        'silver_prefix'
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    bronze_bucket = args['bronze_bucket']
    silver_bucket = args['silver_bucket']
    season = args['run_season']
    
    # Strip any stray slashes to prevent double slashes in the S3 URI
    bronze_prefix = args['bronze_prefix'].strip('/')
    silver_prefix = args['silver_prefix'].strip('/')

    logger.info(f"=== STARTING GLUE JOB RUN FOR SEASON: {season} ===")
    target_parsers = GLOBAL_PARSERS if season == "global" else SEASON_PARSERS

    for endpoint, parser_func in target_parsers.items():
        logger.info(f"--- Processing {endpoint} ---")
        
        # 2. Build paths dynamically using the .env values
        input_path = f"s3://{bronze_bucket}/{bronze_prefix}/{endpoint}/{season}/"
        
        try:
            raw_df = spark.read.option("multiline", "true").json(input_path)
            input_count = raw_df.count()
            logger.info(f"[{endpoint}] Read {input_count} raw JSON records from {input_path}")

            if input_count == 0:
                logger.warning(f"[{endpoint}] Zero records found. Skipping transformation.")
                continue
            
            flattened_df = parser_func(raw_df)
            output_count = flattened_df.count()
            logger.info(f"[{endpoint}] Transformed into {output_count} flattened rows.")
            
            # 3. Output path uses the silver values from .env
            output_path = f"s3://{silver_bucket}/{silver_prefix}/{endpoint}/season={season}/"
            
            flattened_df.write \
                .mode("overwrite") \
                .format("parquet") \
                .option("compression", "snappy") \
                .save(output_path)
                
            logger.info(f"[{endpoint}] Successfully wrote Parquet to {output_path}")

        except Exception as e:
            logger.error(f"[{endpoint}] FATAL ERROR processing {input_path}: {str(e)}")
            raise e

    job.commit()
    logger.info(f"=== COMPLETED GLUE JOB RUN FOR SEASON: {season} ===")

if __name__ == "__main__":
    main()