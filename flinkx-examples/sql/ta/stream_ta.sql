CREATE TABLE source
(
    id        INT,
    name      STRING,
    money     DECIMAL(32, 2),
    dateone   timestamp,
    age       bigint,
    datethree timestamp,
    datesix   timestamp(6),
    datenigth timestamp(9)

) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '10' -- 输入条数，默认无限
     -- ,'rows-per-second' = '1'  每秒输入条数，默认不限制
      );

CREATE TABLE sink
(
    `#account_id`        String,
    name      STRING,
    money     DECIMAL(32, 2),
    `#time`   timestamp,
    age       bigint,
    datethree timestamp,
    datesix   timestamp(6),
    datenigth timestamp(9),
    `#event_name` STRING
) WITH (
      'connector' = 'ta-x',
      'pushUrl'='http://dev-ta1:8991/logbus',
      'appid'= '1a9960b4b5b642809b6b5e3814bb5b02',
      'thread'= '3',
      'compress'= 'none',
      'type'='track'
      );

insert into sink
select cast(id as string),name,money,dateone,age,datethree,datesix,datenigth,'testlog'
from source where ;
