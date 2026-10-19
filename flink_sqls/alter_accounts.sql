alter  table `cdc.public.accounts` SET  (
    'value.format' = 'avro-registry', 
    'changelog.mode' = 'append')