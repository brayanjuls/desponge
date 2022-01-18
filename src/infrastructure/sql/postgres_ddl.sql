CREATE SCHEMA source;

CREATE TABLE IF NOT EXISTS source.tweets (
    id varchar not null,
    text varchar,
    url varchar not null,
    lang varchar,
    created_at date,
    author_name varchar,
    author_user_name varchar,
    author_email varchar,
    retweeted boolean,
    primary key (id)
);
CREATE UNIQUE INDEX tweets_url_index on source.tweets (url);


CREATE TABLE IF NOT EXISTS source.tweet_words(
    id  serial primary key,
    word varchar unique,
    is_de_content boolean
);


CREATE TABLE IF NOT EXISTS source.de_categories(
    id serial primary key,
    key_word varchar,
    category varchar
);
CREATE UNIQUE INDEX de_categories_word_index on source.de_categories (key_word,category);

-- DROP TABLE source.tweets;
-- DROP TABLE source.tweet_words;
-- DROP TABLE source.de_categories;
