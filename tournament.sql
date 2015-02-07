-- Player table
create table player(
    pid serial primary key,
    name text
);

--Match table
create table match(
    winner int references player (pid),
    loser int references player (pid)
);