-- first: create a connection in BQ

-- Create the embedding model
CREATE OR REPLACE MODEL `genaifordata.bqmdm.user_attributes_embeddings`
REMOTE WITH CONNECTION `projects/genaifordata/locations/us/connections/gemini`
OPTIONS (ENDPOINT = 'text-embedding-004');



-- Facebook subview
CREATE OR REPLACE VIEW `genaifordata.bqmdm.facebook_user_view` AS
SELECT
  d.alias, d.bio, d.email, d.region, d.city,
  ARRAY_AGG(STRUCT(f.post, f.likes, f.shares, f.comments, f.date)) AS activities
FROM
  `genaifordata.bqmdm.facebook_dimension` d
LEFT JOIN
  `genaifordata.bqmdm.facebook_fact` f
ON
  d.alias = f.alias
GROUP BY
  d.alias, d.bio, d.email, d.region, d.city;

-- Twitter subview
CREATE OR REPLACE VIEW `genaifordata.bqmdm.twitter_user_view` AS
SELECT
  d.alias, d.bio, d.email, d.region, d.city,
  ARRAY_AGG(STRUCT(f.post, f.likes, f.shares, f.comments, f.date)) AS activities
FROM
  `genaifordata.bqmdm.twitter_dimension` d
LEFT JOIN
  `genaifordata.bqmdm.twitter_fact` f
ON
  d.alias = f.alias
GROUP BY
  d.alias, d.bio, d.email, d.region, d.city;

-- LinkedIn subview
CREATE OR REPLACE VIEW `genaifordata.bqmdm.linkedin_user_view` AS
SELECT
  d.alias, d.bio, d.email, d.region, d.city,
  ARRAY_AGG(STRUCT(f.post, f.likes, f.shares, f.comments, f.date)) AS activities
FROM
  `genaifordata.bqmdm.linkedin_dimension` d
LEFT JOIN
  `genaifordata.bqmdm.linkedin_fact` f
ON
  d.alias = f.alias
GROUP BY
  d.alias, d.bio, d.email, d.region, d.city;

-- GitHub subview
CREATE OR REPLACE VIEW `genaifordata.bqmdm.github_user_view` AS
SELECT
  d.alias, d.bio, d.email, d.region, d.city,
  ARRAY_AGG(STRUCT(f.post AS repository, f.likes AS stars, f.shares AS forks, f.comments AS issues, f.date)) AS activities
FROM
  `genaifordata.bqmdm.github_dimension` d
LEFT JOIN
  `genaifordata.bqmdm.github_fact` f
ON
  d.alias = f.alias
GROUP BY
  d.alias, d.bio, d.email, d.region, d.city;

-- Instagram subview
CREATE OR REPLACE VIEW `genaifordata.bqmdm.instagram_user_view` AS
SELECT
  d.alias, d.bio, d.email, d.region, d.city,
  ARRAY_AGG(STRUCT(f.post AS caption, f.likes, f.shares AS saves, f.comments, f.date)) AS activities
FROM
  `genaifordata.bqmdm.instagram_dimension` d
LEFT JOIN
  `genaifordata.bqmdm.instagram_fact` f
ON
  d.alias = f.alias
GROUP BY
  d.alias, d.bio, d.email, d.region, d.city;


-- Final combined view for embeddings
CREATE OR REPLACE VIEW `genaifordata.bqmdm.all_platforms_user_view` AS
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
FROM `genaifordata.bqmdm.facebook_user_view`

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
FROM `genaifordata.bqmdm.twitter_user_view`

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
FROM `genaifordata.bqmdm.linkedin_user_view`

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
FROM `genaifordata.bqmdm.github_user_view`

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
FROM `genaifordata.bqmdm.instagram_user_view`;



-- Create table with embeddings
CREATE OR REPLACE TABLE `genaifordata.bqmdm.all_platforms_user_embedding` AS

WITH embeddings AS (
  SELECT
    *
  FROM
    ML.GENERATE_EMBEDDING(
      MODEL `genaifordata.bqmdm.user_attributes_embeddings`,
      TABLE `genaifordata.bqmdm.all_platforms_user_view`,
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
FROM
  embeddings e;


-- Create a query to search for a user of choice

WITH query_user AS (
  SELECT alias, embedding
  FROM `genaifordata.bqmdm.all_platforms_user_embedding`
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
    distance
  FROM 
    VECTOR_SEARCH(
      TABLE `genaifordata.bqmdm.all_platforms_user_embedding`,
      'embedding',
      (SELECT embedding FROM query_user),
      top_k => 50,
      distance_type => 'COSINE'
    )
),
ranked_results AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY platform ORDER BY distance) AS rank
  FROM vector_search_results
)
SELECT 
  (SELECT alias FROM query_user) AS query_user_alias,
  platform,
  alias AS closest_match_alias,
  bio,
  email,
  region,
  city,
  distance
FROM 
  ranked_results
WHERE 
  rank = 1
ORDER BY 
  distance;

-- Create a master record for the user of choice.

CREATE OR REPLACE TABLE `genaifordata.bqmdm.all_platforms_master_record` AS

WITH query_user AS (
  SELECT alias, embedding
  FROM `genaifordata.bqmdm.all_platforms_user_embedding`
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
      TABLE `genaifordata.bqmdm.all_platforms_user_embedding`,
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

-- Create a general master table for all users.
CREATE OR REPLACE TABLE `genaifordata.bqmdm.all_platforms_master_records` AS

WITH hashed_records AS (
  SELECT
    *,
    TO_HEX(MD5(CONCAT(IFNULL(alias, ''), IFNULL(bio, ''), IFNULL(email, ''), IFNULL(region, ''), IFNULL(city, '')))) AS unique_id
  FROM `genaifordata.bqmdm.all_platforms_user_embedding`
),
all_unique_ids AS (
  SELECT DISTINCT unique_id, embedding
  FROM hashed_records
),
vector_search_results AS (
  SELECT 
    query.unique_id AS query_id,
    base.platform,
    base.alias,
    base.bio,
    base.email,
    base.region,
    base.city,
    base.activities,
    COSINE_DISTANCE(query.embedding, base.embedding) AS distance
  FROM 
    all_unique_ids query
  CROSS JOIN
    hashed_records base
),
ranked_results AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY query_id, platform ORDER BY distance) AS rank
  FROM vector_search_results
),
master_record AS (
  SELECT
    query_id AS master_id,
    ARRAY_AGG(STRUCT(platform, alias, bio, email, region, city, activities, distance, rank)) AS platform_records,
    COUNT(DISTINCT platform) AS platform_count,
    MIN(distance) AS best_match_distance,
    AVG(distance) AS avg_match_distance
  FROM ranked_results
  WHERE rank = 1
  GROUP BY query_id
)
SELECT
  master_id,
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