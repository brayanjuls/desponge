-- Generate words from tweets, afterward manually label them as data engineering related content or not
insert into source.tweet_words(word)
select distinct lower(regexp_replace(s.text,'[^a-zA-Z0-9]','')) from tweets t, unnest(string_to_array(text,' ')) s(token)
ON CONFLICT DO NOTHING;
