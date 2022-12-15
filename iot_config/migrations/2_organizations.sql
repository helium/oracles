create table organizations {
    oui bigserial primary key not null
    owner_wallet_id text not null
    payer_wallet_id text not null
}
