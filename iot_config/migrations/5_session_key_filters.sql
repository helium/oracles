create table session_key_filters (
	oui bigint not null references organizations(oui) on delete cascade,
	devaddr bigint not null,
	session_key text not null,

	inserted_at timestamptz not null default now(),
	updated_at timestamptz not null default now(),

	unique(oui, devaddr, session_key)
);

select trigger_updated_at('session_key_filters');
