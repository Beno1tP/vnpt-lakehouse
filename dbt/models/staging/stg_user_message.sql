-- Step 4.2 test of the 3-part source name; becomes the real staging model in 4.3.
select * from {{ source('silver', 'user_message') }}
