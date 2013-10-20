
CREATE KEYSPACE twitter_sample
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE twitter_sample;

-- Tweet storage
CREATE TABLE tweet (tweet_id bigint PRIMARY KEY, username text, message text, hashtags list<text>);

-- Hashtag counter
DROP TABLE hashtag_count;
CREATE TABLE hashtag_count (
    hashtag			text,
    nof_occurences	counter,
    PRIMARY KEY(hashtag)
);

DROP TABLE hashtag_timeline;
CREATE TABLE hashtag_timeline (
    bucket_id 		text,		-- 2013/06/10/13 for hour 2013/06/10 for day 2013/06 for month 2013 for year
    time_id  		timestamp,	 
    hashtag			text,
    nof_occurences	counter,
    PRIMARY KEY((bucket_id), time_id, hashtag)
);

-- Tweet storage by time
DROP TABLE tweet_timeline;
CREATE TABLE tweet_timeline (
    target_id 	uuid,
    bucket_id 	text,
    time_id		timestamp, 
    tweet_id  	bigint,
    message  	text,
    hashtags	list<text>,
    PRIMARY KEY((target_id, bucket_id), time_id)
);


DROP TABLE tweet_count;
CREATE TABLE tweet_count (
    target_id 	uuid,
    bucket_id 	text,		-- 2013/06/10/13 for hour 2013/06/10 for day 2013/06 for month 2013 for year
    time_id  	timestamp,	-- granualarity 
    key			text,
    nof_tweets	counter,
    PRIMARY KEY((target_id, bucket_id), time_id, key)
);
