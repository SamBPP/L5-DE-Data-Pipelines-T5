"""Data transformation logic."""
import logging
from pipeline.data_utils import (
    tidy_columns, clean_column, clean_gender, clean_number, clean_salary,
    hash_password, infer_dob
)

logger = logging.getLogger(__name__)


def add_education_column(df, mapping):
    """Add or update the education column based on the mapping provided."""
    try:
        if mapping is None:
            logger.warning("No mapping provided for education column.")
        elif 'education' in df.columns:
            df['rqf'] = df['education'].apply(lambda x: mapping.get(str(x), None))
        elif 'rqf' in df.columns:
            df['education'] = df['rqf'].apply(lambda x: mapping.get(str(x), None))
        else:
            logger.warning("No education or RFQ column found in the DataFrame.")
    except Exception as e:
        logger.error(f"Error in add_education_column: {e}")
    return df


def transform_users(df, country_code, column_mapping=None, gender_mapping=None,
                    education_mapping=None, exclusions=None,
                    payment_period=1, int_dial_code='44', currency='GBP'):
    """Transform data in the user DataFrame."""
    try:
        logger.info("Transforming user data...")
        df = tidy_columns(df, column_mapping)

        for col in df.columns:
            if col not in ['password', 'dob']:
                df[col] = df[col].apply(lambda row: clean_column(row, exclusions))

        if 'gender' in df.columns:
            df['gender'] = df['gender'].astype(str)
            df['gender'] = df['gender'].apply(lambda row: clean_gender(row, gender_mapping))

        if 'dob' in df.columns and 'age_last_birthday' in df.columns:
            df['dob'] = df.apply(lambda row: infer_dob(row['dob'],
                                                       row['age_last_birthday']),
                                 axis=1)

        if 'password' in df.columns:
            df['password'] = df['password'].apply(hash_password)

        for col in ['phone', 'mobile']:
            if col in df.columns:
                df[col] = df[col].apply(clean_number)

        df = add_education_column(df, education_mapping)

        if 'salary' in df.columns:
            df['salary'] = df['salary'].apply(lambda x: clean_salary(x, payment_period))

        df['dial_code'] = int_dial_code
        df['currency'] = currency
        df['country_code'] = country_code

    except Exception as e:
        logger.error(f"Error in transform_users: {e}")

    return df
