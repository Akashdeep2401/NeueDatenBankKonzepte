drop database if exists scenario_spotify;
create database scenario_spotify character set = "utf8" collate = "utf8_general_ci";
use scenario_spotify;
show variables like "secure_file_priv";

create table `user`(
  id int primary key auto_increment, 
  `name` varchar(100) not null, 
  gender char(1) check (gender in ("m", "f"))
);
create table song(
  id int primary key auto_increment, 
  title varchar(100) not null, 
  artist varchar(100) not null
);
create table playlist(
  id int primary key auto_increment,
  `name` varchar(100) not null, 
  created_date date not null, 
  owner_id int not null,
  constraint fk_playlist_owner_id foreign key (owner_id) references `user`(id)
);
create table playlist_song(
  id int primary key auto_increment,
  playlist_id int not null,
  song_id int not null,
  position int not null check (position > 0),
  constraint fk_ps_song_id foreign key (song_id) references song(id)
);
create table playlist_follower(
  playlist_id int not null, 
  follower_id int not null,
  primary key (playlist_id, follower_id),
  constraint fk_pf_playlist_id foreign key (playlist_id) references playlist(id),  
  constraint fk_pf_follower_id foreign key (follower_id) references `user`(id)
);


load data infile "./scenario_spotify_user.csv" into table `user` fields terminated by "," ignore 1 lines;
load data infile "./scenario_spotify_song.csv" into table song fields terminated by "," ignore 1 lines;
load data infile "./scenario_spotify_playlist.csv" into table playlist fields terminated by "," ignore 1 lines;
load data infile "./scenario_spotify_playlist_song.csv" into table playlist_song fields terminated by "," ignore 1 lines;
load data infile "./scenario_spotify_playlist_follower.csv" into table playlist_follower fields terminated by "," ignore 1 lines;

select @@secure_file_priv;

set global innodb_buffer_pool_size = 1073741824; -- 1 GB