"""Load user and login data from CSV files and preprocess them."""
import logging
import pandas as pd
from pipeline.data_utils import tidy_columns

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def drop_missing_row(df, thresh=0.5):
    """ Drop rows with more than `thresh` percent missing values."""
    pct_missing = df.isna().sum(axis=1) / df.shape[1]
    df_missing = df[pct_missing > thresh]
    logger.info(f"Rows with more than {round(thresh*100)}% missing values: {df_missing.shape[0]}")
    return df[pct_missing <= thresh]

def load_user_data(filepath, encoding='utf-8'):
    """Load user data from a CSV file."""
    logger.info("Loading user data from %s", filepath)
    df = pd.read_csv(filepath, encoding=encoding)
    df_filt = drop_missing_row(df)
    return df_filt

def load_login_data(filepath, timezone):
    """Load login data from a CSV file and convert timestamps to UTC."""
    logger.info("Loading login data from %s", filepath)
    df = pd.read_csv(filepath)
    df.columns = ['login_id', 'username', 'login_timestamp']
    tidy_columns(df)
    df.drop(columns=['login_id'], inplace=True)
    df['login_timestamp'] = pd.to_datetime(df['login_timestamp'], unit='s', utc=False)
    df['login_timestamp'] = df['login_timestamp'].dt.tz_localize(timezone).dt.tz_convert('UTC')
    return df
