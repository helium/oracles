alter type key_type rename value 'packet_router' to 'router';

alter type key_type add value if not exists 'carrier';

alter type key_type rename to key_role;

alter table registered_keys rename column key_type to key_role;

alter table registered_keys drop constraint registered_keys_pubkey_key;

alter table registered_keys add primary key (pubkey, key_role);
