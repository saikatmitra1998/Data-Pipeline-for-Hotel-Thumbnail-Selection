import json
import argparse
import shutil
import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log, when, lit, rank
from pyspark.sql.window import Window

# Set up logging to capture detailed logs for debugging and monitoring
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_scores(images_df, tags_df):
    try:
        # Constants used in score calculations
        MIN_RES = 160000  # Minimum resolution threshold
        MAX_RES = 2073600  # Maximum resolution threshold
        MIN_AR = 0.3  # Minimum aspect ratio
        MAX_AR = 4.65  # Maximum aspect ratio
        MAX_FRESHNESS_DAY = 10 * 365  # Maximum freshness in days (10 years)
        WEIGHT_RESOLUTION = 6  # Weight for resolution score
        WEIGHT_ASPECT_RATIO = 2  # Weight for aspect ratio score
        WEIGHT_FRESHNESS = 2  # Weight for freshness score
        WEIGHT_TAG_PRIORITY = 3  # Weight for tag priority score

        # Calculate the resolution of images (width * height)
        images_df = images_df.withColumn('Resolution', col('width') * col('height'))
        
        # Calculate the aspect ratio of images (width / height)
        images_df = images_df.withColumn('AR', when(col('height') == 0, None).otherwise(col('width') / col('height')))
        
        # Calculate the score for resolution using logarithmic scaling
        images_df = images_df.withColumn('Score_res', when(col('Resolution') <= 0, 0).otherwise(
            (log(col('Resolution')) - log(lit(MIN_RES))) / (log(lit(MAX_RES)) - log(lit(MIN_RES)))))
        images_df = images_df.withColumn('Score_res', when(col('Score_res') > 1, 1).otherwise(col('Score_res')))

        # Calculate the score for aspect ratio, ensuring it falls within the acceptable range
        images_df = images_df.withColumn('Score_AR', when(col('AR').isNull(), 0).otherwise(
            when((col('AR') >= MIN_AR) & (col('AR') <= MAX_AR), 1).otherwise(0)))

        # Calculate the score for freshness based on the difference from the current date
        today = datetime.now().date()  # Get the current date
        images_df = images_df.withColumn('Score_fresh', 1 + (-1 * (lit(today).cast("timestamp").cast("long") - col('created_at').cast("timestamp").cast("long")) / (MAX_FRESHNESS_DAY * 24 * 60 * 60)))
        images_df = images_df.withColumn('Score_fresh', when(col('Score_fresh') < 0, 0).otherwise(
            when(col('Score_fresh') > 1, 1).otherwise(col('Score_fresh'))))

        # Extract the highest tag probability for each image
        tags_df = tags_df.withColumn('highest_probability', tags_df.tags[0]['probability']).fillna({'highest_probability': 0})

        # Join the tags DataFrame with the images DataFrame on 'image_id'
        images_df = images_df.join(tags_df, 'image_id', 'left_outer')

        # Calculate the overall score for each image by combining the individual scores with their weights
        images_df = images_df.withColumn('Score_image', (
                WEIGHT_RESOLUTION * col('Score_res') +
                WEIGHT_ASPECT_RATIO * col('Score_AR') +
                WEIGHT_FRESHNESS * col('Score_fresh') +
                WEIGHT_TAG_PRIORITY * col('highest_probability')
            ) / (WEIGHT_RESOLUTION + WEIGHT_ASPECT_RATIO + WEIGHT_FRESHNESS + WEIGHT_TAG_PRIORITY))

        return images_df

    except Exception as e:
        # Log any error that occurs during score calculation
        logger.error(f"Error calculating scores: {e}")
        raise

def save_as_jsonl(df, output_path):
    try:
        # Create a temporary path for saving the output
        temp_path = output_path + "_temp"
        df.coalesce(1).write.mode('overwrite').json(temp_path)

        # Find the single output file in the temporary path
        temp_file = [f for f in os.listdir(temp_path) if f.endswith('.json')][0]

        # Move and rename the single output file to the desired location
        shutil.move(os.path.join(temp_path, temp_file), output_path)
        shutil.rmtree(temp_path)

    except Exception as e:
        # Log any error that occurs during file saving
        logger.error(f"Error saving JSONL file: {e}")
        raise

def main(args):
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("Main Image Selection").getOrCreate()

        # Read the input JSONL files into DataFrames
        images_df = spark.read.json(args.images)
        tags_df = spark.read.json(args.tags)
        main_images_df = spark.read.json(args.main_images)

        # Calculate scores for images
        scored_images_df = calculate_scores(images_df, tags_df)

        # Rank images within each hotel group by their scores
        window_spec = Window.partitionBy("hotel_id").orderBy(col("Score_image").desc())
        ranked_images_df = scored_images_df.withColumn("rank", rank().over(window_spec))
        main_images_df = ranked_images_df.filter(col("rank") == 1).select("hotel_id", "image_id", "Score_image")

        # Save the output DataFrames as JSONL files
        save_as_jsonl(main_images_df, args.output_cdc)
        save_as_jsonl(scored_images_df, args.output_snapshot)

        # Calculate and save the pipeline metrics
        metrics = {
            'number_of_images_processed': scored_images_df.count(),
            'number_of_hotels_with_images': main_images_df.select("hotel_id").distinct().count(),
            'number_of_main_images': {
                'newly_elected': main_images_df.count(),  # All selected images are considered newly elected
                'updated': 0,
                'deleted': 0
            }
        }

        with open(args.output_metrics, 'w') as metrics_file:
            json.dump(metrics, metrics_file)

        logger.info("Pipeline execution completed successfully.")
        spark.stop()

    except Exception as e:
        # Log any error that occurs during the pipeline execution
        logger.error(f"Pipeline execution failed: {e}")
        raise

if __name__ == '__main__':
    # Set up argument parser for command-line arguments
    parser = argparse.ArgumentParser(description='Process JSONL files.')
    parser.add_argument('--images', default='data/images.jsonl', help='Path to the images JSONL file.')
    parser.add_argument('--tags', default='data/image_tags.jsonl', help='Path to the image tags JSONL file.')
    parser.add_argument('--main_images', default='data/main_images.jsonl', help='Path to the main images JSONL file.')
    parser.add_argument('--output_cdc', default='output/output_cdc.jsonl', help='Path to the output CDC JSONL file.')
    parser.add_argument('--output_snapshot', default='output/output_snapshot.jsonl', help='Path to the output snapshot JSONL file.')
    parser.add_argument('--output_metrics', default='output/output_metrics.jsonl', help='Path to the output metrics JSONL file.')

    # Parse the command-line arguments
    args = parser.parse_args()
    # Run the main function with the parsed arguments
    main(args)