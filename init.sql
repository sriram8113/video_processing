CREATE TABLE video_processing_status (
    video_key VARCHAR(255) NOT NULL,         -- Unique identifier for the video file
    s3_arrival_time TIMESTAMP NOT NULL,      -- Time when the file was uploaded to S3
    metadata_present VARCHAR(10) DEFAULT 'false', -- Indicates if metadata is available ('true', 'false')
    corruption_check VARCHAR(10) DEFAULT 'false', -- Indicates if the video passed corruption checks ('true', 'false')
    has_audio VARCHAR(10) DEFAULT 'false',  -- Indicates if the video contains audio ('true', 'false')
    has_video VARCHAR(10) DEFAULT 'false',  -- Indicates if the video contains video ('true', 'false')
    quality_rating INTEGER DEFAULT 0,       -- Overall quality rating for the video
    processing_status VARCHAR(50) DEFAULT 'Pending', -- Status of video processing ('Pending', 'Completed', etc.)
    notification_sent VARCHAR(10) DEFAULT 'false', -- Indicates if a notification has been sent ('true', 'false')
    arrival_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Time when the record was created
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Last time the row was updated
    PRIMARY KEY (video_key, s3_arrival_time) -- Composite primary key
);


CREATE TABLE video_metadata (
    id SERIAL PRIMARY KEY,                     -- Unique identifier for each metadata entry
    video_key VARCHAR(255) NOT NULL,           -- Foreign key linking to the video_processing_status table
    s3_arrival_time TIMESTAMP NOT NULL,        -- Matches with video_processing_status for uniqueness
    duration VARCHAR(50),                      -- Duration of the video
    file_size VARCHAR(50),                     -- File size
    codec VARCHAR(50),                         -- Codec used
    resolution VARCHAR(50),                    -- Resolution of the video
    frame_rate VARCHAR(50),                    -- Frame rate
    bitrate VARCHAR(50),                       -- Bitrate of the video
    audio_channels VARCHAR(50),                -- Number of audio channels
    title VARCHAR(255),                        -- Title of the video
    description TEXT,                          -- Description of the video
    tags TEXT,                                 -- Tags (comma-separated)
    creation_date VARCHAR(50),                 -- Creation date of the video
    creator_author VARCHAR(255),               -- Creator/Author
    copyright_info TEXT,                       -- Copyright information
    file_format VARCHAR(50),                   -- File format
    encoding_type VARCHAR(50),                 -- Encoding type
    access_rights TEXT,                        -- Access rights
    version_info VARCHAR(50),                  -- Version information
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- When this metadata was stored
    FOREIGN KEY (video_key, s3_arrival_time) REFERENCES video_processing_status (video_key, s3_arrival_time) ON DELETE CASCADE
);



CREATE TABLE retry_status (
    id SERIAL PRIMARY KEY,                -- Unique identifier for each retry record
    video_key VARCHAR(255) NOT NULL,      -- Video file key, uniquely identifies the file
    retry_count INTEGER DEFAULT 0,        -- Number of retries for the video file
    last_retry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Last retry attempt timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Record creation time
);



CREATE TABLE dlq_messages (
    id SERIAL PRIMARY KEY,
    message_body TEXT,
    receive_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- DROP FUNCTION public.get_counts(timestamp, timestamp);

CREATE OR REPLACE FUNCTION public.get_counts(begin_time timestamp without time zone, end_time timestamp without time zone)
 RETURNS TABLE(total_videos bigint, metadata_present_count bigint, corruption_checks_passed_count bigint, has_audio_count bigint, has_video_count bigint, notifications_sent_count bigint)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT COUNT(*) AS total_videos,
           COUNT(*) FILTER (WHERE metadata_present = 'true') AS metadata_present_count,
           COUNT(*) FILTER (WHERE corruption_check = 'true') AS corruption_checks_passed_count,
           COUNT(*) FILTER (WHERE has_audio = 'true') AS has_audio_count,
           COUNT(*) FILTER (WHERE has_video = 'true') AS has_video_count,
           COUNT(*) FILTER (WHERE notification_sent = 'true') AS notifications_sent_count
    FROM video_processing_status
    WHERE s3_arrival_time BETWEEN begin_time AND end_time;
END;
$function$
;


-- DROP FUNCTION public.get_video_details(timestamp, timestamp);

CREATE OR REPLACE FUNCTION public.get_video_details(begin_time timestamp without time zone, end_time timestamp without time zone)
 RETURNS TABLE(video_key character varying, s3_arrival_time timestamp without time zone, metadata_present character varying, corruption_check character varying, has_audio character varying, has_video character varying, quality_rating integer, processing_status character varying, notification_sent character varying, arrival_time timestamp without time zone, last_updated timestamp without time zone)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT v.video_key, v.s3_arrival_time, v.metadata_present, v.corruption_check, v.has_audio, 
           v.has_video, v.quality_rating, v.processing_status, v.notification_sent, v.arrival_time, v.last_updated
    FROM video_processing_status v
    WHERE v.s3_arrival_time BETWEEN begin_time AND end_time;
END;
$function$
;
