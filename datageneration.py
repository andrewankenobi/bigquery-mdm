import random
import time
from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel
import logging
from datetime import datetime, timedelta
import json
import re
from google.api_core.exceptions import NotFound

# Parametric variables
DATASET_ID = 'genaifordata.bqmdm'
CONNECTION_NAME = 'projects/genaifordata/locations/us/connections/gemini'
EMBEDDING_MODEL = 'text-embedding-004'

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize BigQuery Client
client = bigquery.Client()

# Initialize Gemini model
model = GenerativeModel("gemini-1.5-flash")

def generate_content(prompt):
    try:
        response = model.generate_content(prompt)
        if response.text:
            return response.text
        else:
            logger.warning("Generated content is empty")
            return None
    except Exception as e:
        logger.error(f"Error generating content: {str(e)}")
        return None

def generate_user_profile(figure_name, field, max_retries=3):
    for attempt in range(max_retries):
        prompt = f"""
        Generate a professional alias for {figure_name} for each of the following platforms: Twitter, LinkedIn, Facebook, Instagram, and GitHub.
        Keep the following rules:
        1. Use variations of the original name that are significantly different across platforms.
        2. Maintain consistency with the figure's gender and cultural background.
        3. Include appropriate titles or honorifics related to their field of {field} where relevant.
        4. Create a brief, platform-specific bio for each alias (max 50 words). The bio should be in first person and appropriate for the platform.
        5. Generate a plausible email address for each alias.
        6. Assign a region and city for each alias (can be fictional but plausible).
        7. Ensure all content is safe, professional, and appropriate for all audiences.
        8. Do not include any controversial, offensive, or sensitive information.
        Format the response as a valid JSON object with the following structure:
        {{
            "Twitter": {{"alias": "...", "bio": "...", "email": "...", "region": "...", "city": "..."}},
            "LinkedIn": {{"alias": "...", "bio": "...", "email": "...", "region": "...", "city": "..."}},
            "Facebook": {{"alias": "...", "bio": "...", "email": "...", "region": "...", "city": "..."}},
            "Instagram": {{"alias": "...", "bio": "...", "email": "...", "region": "...", "city": "..."}},
            "GitHub": {{"alias": "...", "bio": "...", "email": "...", "region": "...", "city": "..."}}
        }}
        Ensure that the output is a properly formatted, valid JSON string without any additional text or formatting.
        """
        response = generate_content(prompt)
        if response is None:
            logger.warning(f"Attempt {attempt + 1} failed to generate content for {figure_name}")
            continue
        
        try:
            # Remove any potential markdown formatting and extra whitespace
            json_str = re.sub(r'```json\s*|\s*```', '', response.strip())
            json_str = re.sub(r'\s+', ' ', json_str)
            data = json.loads(json_str)
            
            # Validate the structure of the generated data
            if all(platform in data for platform in platforms) and \
               all(all(key in data[platform] for key in ['alias', 'bio', 'email', 'region', 'city']) for platform in platforms):
                data['original_figure'] = figure_name
                return data
            else:
                logger.warning(f"Attempt {attempt + 1} returned incomplete data for {figure_name}")
        except json.JSONDecodeError as e:
            logger.warning(f"Attempt {attempt + 1} failed to parse JSON for {figure_name}: {str(e)}")
            logger.debug(f"Failed JSON: {json_str}")
        except Exception as e:
            logger.warning(f"Unexpected error in attempt {attempt + 1} for {figure_name}: {str(e)}")
    
    logger.error(f"Failed to generate profile for {figure_name} after {max_retries} attempts")
    # Generate a fallback profile
    fallback_profile = {platform: {
        "alias": f"{figure_name} on {platform}",
        "bio": f"Fictional bio for {figure_name}, an expert in {field}.",
        "email": f"{figure_name.lower().replace(' ', '.')}@{platform.lower()}.com",
        "region": "Unknown Region",
        "city": "Unknown City"
    } for platform in platforms}
    fallback_profile['original_figure'] = figure_name
    return fallback_profile

users = [
    {"figure": "Alan Turing", "field": "Computer Science"},
    {"figure": "James Watson", "field": "Biology"},
    {"figure": "Lord Kelvin", "field": "Physics"},
    {"figure": "James Clerk Maxwell", "field": "Physics"},
    {"figure": "Stephen Hawking", "field": "Theoretical Physics"},
    {"figure": "Isaac Newton", "field": "Physics and Mathematics"},
]

platforms = ["Twitter", "LinkedIn", "Facebook", "Instagram", "GitHub"]

def generate_user_profiles():
    user_profiles = []
    for user in users:
        profile = generate_user_profile(user["figure"], user["field"])
        if profile:
            profile['original_figure'] = user["figure"]
            user_profiles.append(profile)
        else:
            logger.warning(f"Using fallback profile for {user['figure']}")
            fallback_profile = {platform: {
                "alias": f"{user['figure']} on {platform}",
                "bio": f"Fictional bio for {user['figure']} on {platform}",
                "email": f"{user['figure'].lower().replace(' ', '.')}@{platform.lower()}.com",
                "region": "Region X",
                "city": "City Y"
            } for platform in platforms}
            fallback_profile['original_figure'] = user["figure"]
            user_profiles.append(fallback_profile)
    logger.info(f"Generated {len(user_profiles)} user profiles")
    return user_profiles

def generate_social_media_activity(user_profile, platform, min_posts=10, max_posts=45):
    activities = []
    for _ in range(random.randint(min_posts, max_posts)):
        prompt = f"""
        Generate a professional social media post for {user_profile['original_figure']} under the alias {user_profile[platform]['alias']}, 
        reflecting {platform}-style content. The post should be no longer than 280 characters for Twitter, 
        or up to 500 characters for other platforms. Include relevant professional hashtags where appropriate.
        Only generate the content for {platform}, do not include content for other platforms.
        Ensure the content is safe, appropriate for all audiences, and free from controversial or sensitive topics.
        Focus on professional achievements, scientific discoveries, or educational content related to {user_profile['original_figure']}'s field.
        Format the response as a valid JSON object with a single key 'post' containing the generated content.
        Ensure that the output is a properly formatted, valid JSON string without any additional text or formatting.
        Do not include any markdown formatting or code block indicators.
        """
        response = generate_content(prompt)
        if response:
            try:
                # Remove any potential markdown formatting
                json_str = response.strip().replace('```json', '').replace('```', '').strip()
                post_data = json.loads(json_str)
                post_content = post_data.get('post', '').strip()
                if post_content:
                    activities.append({
                        "platform": platform,
                        "alias": user_profile[platform]['alias'],
                        "post": post_content,
                        "likes": random.randint(0, 500),
                        "shares": random.randint(0, 200),
                        "comments": random.randint(0, 100),
                        "date": (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
                    })
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON for post: {str(e)}")
                logger.debug(f"Failed JSON: {json_str}")
    return activities

dimension_schema = [
    bigquery.SchemaField("alias", "STRING"),
    bigquery.SchemaField("bio", "STRING"),
    bigquery.SchemaField("original_figure", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("city", "STRING"),
]

fact_schema = [
    bigquery.SchemaField("platform", "STRING"),
    bigquery.SchemaField("alias", "STRING"),
    bigquery.SchemaField("post", "STRING"),
    bigquery.SchemaField("likes", "INTEGER"),
    bigquery.SchemaField("shares", "INTEGER"),
    bigquery.SchemaField("comments", "INTEGER"),
    bigquery.SchemaField("date", "TIMESTAMP"),
]

def drop_table_if_exists(table_id):
    try:
        client.delete_table(table_id)
        logger.info(f"Table {table_id} dropped successfully.")
    except Exception as e:
        logger.info(f"Table {table_id} does not exist or could not be dropped: {str(e)}")

def create_or_get_table(table_id, schema):
    drop_table_if_exists(table_id)
    table = bigquery.Table(table_id, schema=schema)
    try:
        table = client.create_table(table)
        logger.info(f"Table {table_id} created successfully.")
    except Exception as e:
        logger.warning(f"Error creating table {table_id}: {str(e)}")
        logger.info(f"Attempting to get existing table {table_id}")
        table = client.get_table(table_id)
    
    # Ensure the table exists
    retries = 0
    max_retries = 5
    while retries < max_retries:
        try:
            client.get_table(table_id)
            logger.info(f"Table {table_id} confirmed to exist.")
            return table
        except Exception:
            logger.warning(f"Table {table_id} not found. Retrying in 5 seconds...")
            time.sleep(5)
            retries += 1
    
    raise Exception(f"Failed to create or get table {table_id} after {max_retries} attempts")

def insert_rows(table, rows, max_wait_time=60):
    if not rows:
        logger.warning(f"No rows to insert into {table.table_id}")
        return

    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        try:
            errors = client.insert_rows_json(table, rows)
            if errors:
                logger.error(f"Errors inserting rows into {table.table_id}:")
                for i, error in enumerate(errors):
                    logger.error(f"Row {i}: {error}")
                logger.debug(f"Problematic rows: {rows}")
            else:
                logger.info(f"Successfully inserted {len(rows)} rows into {table.table_id}")
                return
        except NotFound:
            logger.warning(f"Table {table.table_id} not found. Retrying in 5 seconds...")
            time.sleep(5)
    
    logger.error(f"Failed to insert rows into {table.table_id} after {max_wait_time} seconds")

def execute_sql_statements():
    logger.info("Executing SQL statements to create models, views, and tables")
    
    sql_statements = f"""
    -- Create the embedding model
    CREATE OR REPLACE MODEL `{DATASET_ID}.user_attributes_embeddings`
    REMOTE WITH CONNECTION `{CONNECTION_NAME}`
    OPTIONS (ENDPOINT = '{EMBEDDING_MODEL}');

    -- Facebook subview
    CREATE OR REPLACE VIEW `{DATASET_ID}.facebook_user_view` AS
    SELECT
      d.alias, d.bio, d.email, d.region, d.city,
      ARRAY_AGG(STRUCT(f.post, f.likes, f.shares, f.comments, f.date)) AS activities
    FROM
      `{DATASET_ID}.facebook_dimension` d
    LEFT JOIN
      `{DATASET_ID}.facebook_fact` f
    ON
      d.alias = f.alias
    GROUP BY
      d.alias, d.bio, d.email, d.region, d.city;

    -- Twitter subview
    CREATE OR REPLACE VIEW `{DATASET_ID}.twitter_user_view` AS
    SELECT
      d.alias, d.bio, d.email, d.region, d.city,
      ARRAY_AGG(STRUCT(f.post, f.likes, f.shares, f.comments, f.date)) AS activities
    FROM
      `{DATASET_ID}.twitter_dimension` d
    LEFT JOIN
      `{DATASET_ID}.twitter_fact` f
    ON
      d.alias = f.alias
    GROUP BY
      d.alias, d.bio, d.email, d.region, d.city;

    -- LinkedIn subview
    CREATE OR REPLACE VIEW `{DATASET_ID}.linkedin_user_view` AS
    SELECT
      d.alias, d.bio, d.email, d.region, d.city,
      ARRAY_AGG(STRUCT(f.post, f.likes, f.shares, f.comments, f.date)) AS activities
    FROM
      `{DATASET_ID}.linkedin_dimension` d
    LEFT JOIN
      `{DATASET_ID}.linkedin_fact` f
    ON
      d.alias = f.alias
    GROUP BY
      d.alias, d.bio, d.email, d.region, d.city;

    -- GitHub subview
    CREATE OR REPLACE VIEW `{DATASET_ID}.github_user_view` AS
    SELECT
      d.alias, d.bio, d.email, d.region, d.city,
      ARRAY_AGG(STRUCT(f.post AS repository, f.likes AS stars, f.shares AS forks, f.comments AS issues, f.date)) AS activities
    FROM
      `{DATASET_ID}.github_dimension` d
    LEFT JOIN
      `{DATASET_ID}.github_fact` f
    ON
      d.alias = f.alias
    GROUP BY
      d.alias, d.bio, d.email, d.region, d.city;

    -- Instagram subview
    CREATE OR REPLACE VIEW `{DATASET_ID}.instagram_user_view` AS
    SELECT
      d.alias, d.bio, d.email, d.region, d.city,
      ARRAY_AGG(STRUCT(f.post AS caption, f.likes, f.shares AS saves, f.comments, f.date)) AS activities
    FROM
      `{DATASET_ID}.instagram_dimension` d
    LEFT JOIN
      `{DATASET_ID}.instagram_fact` f
    ON
      d.alias = f.alias
    GROUP BY
      d.alias, d.bio, d.email, d.region, d.city;

    -- Final combined view for embeddings
    CREATE OR REPLACE VIEW `{DATASET_ID}.all_platforms_user_view` AS
    SELECT
      'Facebook' AS platform,
      *,
      CONCAT(
        'Facebook ', 
        alias, ' ', 
        bio, ' ', 
        email, ' ', 
        region, ' ', 
        city, ' ', 
        (SELECT STRING_AGG(post, ' ') FROM UNNEST(activities))
      ) AS content
    FROM `{DATASET_ID}.facebook_user_view`

    UNION ALL

    SELECT
      'Twitter' AS platform,
      *,
      CONCAT(
        'Twitter ', 
        alias, ' ', 
        bio, ' ', 
        email, ' ', 
        region, ' ', 
        city, ' ', 
        (SELECT STRING_AGG(post, ' ') FROM UNNEST(activities))
      ) AS content
    FROM `{DATASET_ID}.twitter_user_view`

    UNION ALL

    SELECT
      'LinkedIn' AS platform,
      *,
      CONCAT(
        'LinkedIn ', 
        alias, ' ', 
        bio, ' ', 
        email, ' ', 
        region, ' ', 
        city, ' ', 
        (SELECT STRING_AGG(post, ' ') FROM UNNEST(activities))
      ) AS content
    FROM `{DATASET_ID}.linkedin_user_view`

    UNION ALL

    SELECT
      'GitHub' AS platform,
      *,
      CONCAT(
        'GitHub ', 
        alias, ' ', 
        bio, ' ', 
        email, ' ', 
        region, ' ', 
        city, ' ', 
        (SELECT STRING_AGG(repository, ' ') FROM UNNEST(activities))
      ) AS content
    FROM `{DATASET_ID}.github_user_view`

    UNION ALL

    SELECT
      'Instagram' AS platform,
      *,
      CONCAT(
        'Instagram ', 
        alias, ' ', 
        bio, ' ', 
        email, ' ', 
        region, ' ', 
        city, ' ', 
        (SELECT STRING_AGG(caption, ' ') FROM UNNEST(activities))
      ) AS content
    FROM `{DATASET_ID}.instagram_user_view`;

    -- Create table with embeddings
    CREATE OR REPLACE TABLE `{DATASET_ID}.all_platforms_user_embedding` AS
    WITH embeddings AS (
      SELECT *
      FROM ML.GENERATE_EMBEDDING(
        MODEL `{DATASET_ID}.user_attributes_embeddings`,
        TABLE `{DATASET_ID}.all_platforms_user_view`,
        STRUCT(TRUE AS flatten_json_output, 'CLASSIFICATION' AS task_type)
      )
    )
    SELECT
      e.platform,
      e.alias,
      e.bio,
      e.email,
      e.region,
      e.city,
      e.activities,
      e.ml_generate_embedding_result AS embedding
    FROM embeddings e;

    -- Create a master record for the user of choice
    CREATE OR REPLACE TABLE `{DATASET_ID}.all_platforms_master_record` AS
    WITH query_user AS (
      SELECT alias, embedding
      FROM `{DATASET_ID}.all_platforms_user_embedding`
      WHERE lower(alias) LIKE '%aturing%'
      LIMIT 1
    ),
    vector_search_results AS (
      SELECT 
        base.platform,
        base.alias,
        base.bio,
        base.email,
        base.region,
        base.city,
        base.activities,
        distance
      FROM 
        VECTOR_SEARCH(
          TABLE `{DATASET_ID}.all_platforms_user_embedding`,
          'embedding',
          (SELECT embedding FROM query_user),
          top_k => 5,
          distance_type => 'COSINE'
        )
    ),
    ranked_results AS (
      SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY platform ORDER BY distance) AS rank
      FROM vector_search_results
    ),
    master_record AS (
      SELECT
        (SELECT alias FROM query_user) AS master_alias,
        ARRAY_AGG(STRUCT(platform, alias, bio, email, region, city, activities, distance, rank)) AS platform_records,
        COUNT(DISTINCT platform) AS platform_count,
        MIN(distance) AS best_match_distance,
        AVG(distance) AS avg_match_distance
      FROM ranked_results
      WHERE rank = 1
    )
    SELECT
      master_alias,
      platform_records,
      platform_count,
      best_match_distance,
      avg_match_distance,
      CASE
        WHEN platform_count >= 3 AND avg_match_distance < 0.2 THEN 'High'
        WHEN platform_count >= 2 AND avg_match_distance < 0.3 THEN 'Medium'
        ELSE 'Low'
      END AS confidence_score
    FROM master_record;
    """
    
    # Split the SQL statements and execute them one by one
    statements = sql_statements.split(';')
    for statement in statements:
        if statement.strip():
            try:
                query_job = client.query(statement)
                query_job.result()  # Wait for the job to complete
                logger.info(f"Successfully executed SQL statement: {statement[:50]}...")
            except Exception as e:
                logger.error(f"Error executing SQL statement: {str(e)}")
                logger.error(f"Problematic statement: {statement}")

    logger.info("Finished executing all SQL statements")

def main():
    log_file = 'data_generation.log'
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    logger.info("Starting data generation process")

    user_profiles = generate_user_profiles()
    logger.info(f"Generated {len(user_profiles)} user profiles")

    for platform in platforms:
        dimension_table_id = f"{DATASET_ID}.{platform.lower()}_dimension"
        fact_table_id = f"{DATASET_ID}.{platform.lower()}_fact"
        
        dimension_table = create_or_get_table(dimension_table_id, dimension_schema)
        fact_table = create_or_get_table(fact_table_id, fact_schema)
        
        dimension_rows = []
        fact_rows = []
        
        for user_profile in user_profiles:
            platform_data = user_profile[platform]
            dimension_rows.append({
                "alias": platform_data['alias'],
                "bio": platform_data['bio'],
                "original_figure": user_profile['original_figure'],
                "email": platform_data['email'],
                "region": platform_data['region'],
                "city": platform_data['city']
            })
            
            activities = generate_social_media_activity(user_profile, platform)
            fact_rows.extend(activities)

        logger.info(f"Generated {len(dimension_rows)} dimension rows and {len(fact_rows)} fact rows for {platform}")
        
        insert_rows(dimension_table, dimension_rows)
        insert_rows(fact_table, fact_rows)

    logger.info("Data generation and upload complete.")
    execute_sql_statements()

    logger.info(f"Log file saved to: {log_file}")

    # Remove the file handler to avoid duplicate logs in future runs
    logger.removeHandler(file_handler)

if __name__ == "__main__":
    main()
