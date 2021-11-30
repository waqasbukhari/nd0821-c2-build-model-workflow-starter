#!/usr/bin/env python
"""
Download from W&B the raw dataset, ap basic preprocessing and export the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Loading the input artifact. ")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    # Removing the outliers
    logger.info("The dataset (artifact) is loaded")
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    logger.info("Converting string to datetime column")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Saving the dataframe. 
    logger.info("Saving the dataframe. ")
    df.to_csv("clean_sample.csv", index=False)
    
    # Logging the dataframe to artifact and uploading. 
    logger.info("Creating an artifact and attaching cleaned dataset to it. ")
    artifact = wandb.Artifact(args.output_artifact,
                              type = args.output_type, 
                              description = args.output_description
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    run.finish()






if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str, 
        help="Input data that has to be processed.",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str ,
        help="Processed data after the cleaning steps." ,
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str ,
        help="Type of the output artifact" ,
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str ,
        help="Description of the output artifact" ,
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Possible minimum price of a real estate listing. " ,
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float ,
        help="Possible maximum price of a real estate listing. " ,
        required=True
    )


    args = parser.parse_args()

    go(args)
