/*Andrea Delgado Galisteo*/
/*Creación BBDD*/

/*Lo primero es asegurarnos de hacer drop a la base de datos y tablas por si existieran ya que 
queremos definirlas más abajo. También creamos el schema en el que vamos a trabajar*/

drop database if exists TFM;
create database TFM;
use TFM;
/*Parte de borrar tablas*/
drop table if exists comunidades_autonomas;
drop table if exists provincias;
drop table if exists MUNICIPIOS;
drop table if exists total_municipios;
drop table if exists ZIPs;
drop table if exists PRO_GEOMETRY;
drop table if exists MUN_GEOMETRY;

drop table if exists estaciones_AEMET;
drop table if exists estaciones_NOAA;
drop table if exists AEMET_observaciones;
drop table if exists NOAA_observaciones;

drop view if exists estaciones_municipios;
drop view if exists estaciones_municipios;

create table comunidades_autonomas(
codauto char(2) not null,
comunidad_autonoma varchar(200) unique key not null,
unique(codauto, comunidad_autonoma),
primary key(codauto)
);

create table provincias(
cpro char(2) not null,
provincia varchar(200) unique key not null,
unique(cpro, provincia),
primary key(cpro)
);


create table MUNICIPIOS(
ine_mun_id char(5) not null,
cpro char(2) not null,
codauto char(2) not null,
nombre varchar(500) not null,
capital varchar(1000) not null,
latitud varchar(1000) not null,
longitud varchar(1000) not null,
latitud_dec float,
longitud_dec float,
altitud int,
num_habitantes int,
primary key(ine_mun_id),
foreign key(codauto) references comunidades_autonomas(codauto),
foreign key(cpro) references provincias(cpro));



create table ZIPs(
zip_mun_id int not null auto_increment,
ine_mun_id char(5) not null,
zip char(5) not null,
primary key(zip_mun_id),
unique (ine_mun_id, zip),
foreign key(ine_mun_id) references municipios(ine_mun_id));


create table PRO_GEOMETRY(
geo_pro_id int not null auto_increment,
cpro char(2) not null,
pro_geometry longtext not null,
primary key(geo_pro_id),
foreign key(cpro) references provincias(cpro) on delete cascade);


create table MUN_GEOMETRY(
geo_mun_id int not null auto_increment,
ine_mun_id char(5) not null,
mun_geometry longtext not null,
primary key(geo_mun_id),
foreign key(ine_mun_id) references municipios(ine_mun_id) on delete cascade);


create table total_municipios(
ine_mun_id char(5) not null,
cpro char(2) not null,
codauto char(2) not null,
nombre varchar(500) not null,
capital varchar(1000) not null,
provincia varchar(500) not null,
comunidad_autonoma varchar(500) not null,
latitud varchar(1000) not null,
longitud varchar(1000) not null,
latitud_dec float,
longitud_dec float,
altitud int,
num_habitantes int,
ZIPs_array longtext,
mun_geometry longtext,
primary key(ine_mun_id),
foreign key(codauto) references comunidades_autonomas(codauto),
foreign key(cpro) references provincias(cpro),
foreign key(comunidad_autonoma) references comunidades_autonomas(comunidad_autonoma),
foreign key(provincia) references provincias(provincia));



create table estaciones_AEMET(
aemet_id varchar(10) not null,
aemet_nombre varchar(500) not null,
ine_mun_id char(5) not null,
latitud varchar(1000) not null,
longitud varchar(1000) not null,
latitud_dec float,
longitud_dec float,
altitud int,
primary key (aemet_id),
foreign key(ine_mun_id) references municipios(ine_mun_id) on delete cascade);



create table estaciones_NOAA(
noaa_id varchar(100) not null,
noaa_nombre varchar(500) not null,
ine_mun_id char(5) not null,
latitud_dec float,
longitud_dec float,
altitud int,
primary key (noaa_id),
foreign key(ine_mun_id) references municipios(ine_mun_id) on delete cascade);

create table AEMET_observaciones (
aemet_obs_id int not null auto_increment,
aemet_id varchar(10) not null,
fecha date,
tmed float,
prec float,
tmin float,
horatmin time,
tmax float,
horatmax time, 
dir float,
velmedia float,
racha float,
horaracha time,
presmax float,
horapresmax int,
presmin float,
horapresmin int,
sol float,
primary key (aemet_obs_id),
unique (aemet_id, fecha),
foreign key(aemet_id) references estaciones_aemet(aemet_id) on delete cascade);

create table NOAA_observaciones (
noaa_obs_id int not null auto_increment,
noaa_id varchar(100) not null,
fecha date,
tmax float,
tmin float,
prcp float,
tavg float,
snwd float,
primary key (noaa_obs_id),
unique (noaa_id, fecha),
foreign key(noaa_id) references estaciones_noaa(noaa_id) on delete cascade);


create view estaciones_municipios(ine_mun_id, aemet_id, noaa_id, nr_estaciones) as
select 
	ine_mun_id,
    ifnull(group_concat(distinct(ea.aemet_id)), '-'),
    ifnull(group_concat(distinct(en.noaa_id)), '-'),
    ifnull((ifnull(aemet_counter,0)+ifnull(noaa_counter,0)),0) as nr_estaciones
from
	municipios as m
		left join 
	estaciones_aemet as ea using (ine_mun_id)
		left join 
	estaciones_noaa as en using (ine_mun_id)
		left join( select count(*) as aemet_counter, ine_mun_id 
				   from estaciones_aemet 
                   group by ine_mun_id) as aemet_nr using (ine_mun_id)
        left join( select count(*) as noaa_counter, ine_mun_id 
				   from estaciones_noaa
                   group by ine_mun_id) as noaa_nr using (ine_mun_id)
group by ine_mun_id
having nr_estaciones>0;    


create view aemet_medias_provincias_anualmensual (cpro, year_monnths, provincia, mean_prec, mean_tmax, mean_tmin, mean_racha, meanpresmax, meanpresmin, delta_mean_temp, delta_mean_pres) as 
select *, round((mean_tmax-mean_tmin),2) as delta_mean_temp ,round((mean_presmax-mean_presmin),2) as delta_mean_pres
from provincias 
left join (select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(prec), 2) as mean_prec
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  prec is not null 
group by date_format(fecha, '%Y-%m'), cpro) as mean_prec_pro using (cpro)
left join (select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(tmax), 2) as mean_tmax
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  tmax is not null 
group by date_format(fecha, '%Y-%m'), cpro) as mean_tmax_pro using (cpro, year_months)
left join (select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(tmin), 2) as mean_tmin
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  tmin is not null 
group by date_format(fecha, '%Y-%m'), cpro) as mean_tmin_pro using (cpro, year_months)
left join (select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(racha), 2) as mean_racha
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  racha is not null 
group by date_format(fecha, '%Y-%m'), cpro) as mean_racha_pro using (cpro, year_months)
left join (select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(presmax), 2) as mean_presmax
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  presmax is not null 
group by date_format(fecha, '%Y-%m'), cpro) as mean_presmax_pro using (cpro, year_months)
left join (select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(presmin), 2) as mean_presmin
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  presmax is not null 
group by date_format(fecha, '%Y-%m'), cpro) as mean_presmin_pro using (cpro, year_months)
order by  cpro;



create view aemet_medias_provincias_mensual (cpro, year_monnths, provincia, mean_prec, mean_tmax, mean_tmin, mean_racha, meanpresmax, meanpresmin, delta_mean_temp, delta_mean_pres) as 
select *, round((mean_tmax-mean_tmin),2) as delta_mean_temp ,round((mean_presmax-mean_presmin),2) as delta_mean_pres
from provincias 
left join (select cpro, date_format(fecha, '%m') as months, round(avg(prec), 2) as mean_prec
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  prec is not null 
group by date_format(fecha, '%m'), cpro) as mean_prec_pro using (cpro)
left join (select cpro, date_format(fecha, '%m') as months, round(avg(tmax), 2) as mean_tmax
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  tmax is not null 
group by date_format(fecha, '%m'), cpro) as mean_tmax_pro using (cpro, months)
left join (select cpro, date_format(fecha, '%m') as months, round(avg(tmin), 2) as mean_tmin
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  tmin is not null 
group by date_format(fecha, '%m'), cpro) as mean_tmin_pro using (cpro, months)
left join (select cpro, date_format(fecha, '%m') as months, round(avg(racha), 2) as mean_racha
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  racha is not null 
group by date_format(fecha, '%m'), cpro) as mean_racha_pro using (cpro, months)
left join (select cpro, date_format(fecha, '%m') as months, round(avg(presmax), 2) as mean_presmax
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  presmax is not null 
group by date_format(fecha, '%m'), cpro) as mean_presmax_pro using (cpro, months)
left join (select cpro, date_format(fecha, '%m') as months, round(avg(presmin), 2) as mean_presmin
from AEMET_observaciones as ao
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as ea using (aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where  presmax is not null 
group by date_format(fecha, '%m'), cpro) as mean_presmin_pro using (cpro, months)
order by cpro, months;



create view aemet_vs_noaa (cpro, year_months, provincia, noaa_mean_tmax, aemet_mean_tmax, noaa_mean_tmin, aemet_mean_tmin, noaa_mean_prcp, aemet_mean_prcp, delta_tmax, delta_tmin, delta_prcp) as
select *, 
round((ifnull(noaa_mean_tmax,0)-ifnull(aemet_mean_tmax,0)),2) as delta_tmax, 
round((ifnull(noaa_mean_tmin,0)-ifnull(aemet_mean_tmin,0)),2) as delta_tmin, 
round((ifnull(noaa_mean_prcp,0)-ifnull(aemet_mean_prcp,0)),2) as delta_prcp
from provincias 
left join (
select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(tmax), 2) as noaa_mean_tmax
from NOAA_observaciones as no
left join (select noaa_id, ine_mun_id from estaciones_NOAA) as en using(noaa_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where tmax is not null 
group by date_format(fecha, '%Y-%m'), cpro) as noaa_t_max using (cpro)
left join (
select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(tmax), 2) as aemet_mean_tmax
from AEMET_observaciones as no
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as en using(aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where tmax is not null 
group by date_format(fecha, '%Y-%m'), cpro) as aemet_t_max using (cpro, year_months)
left join (
select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(tmin), 2) as noaa_mean_tmin
from NOAA_observaciones as no
left join (select noaa_id, ine_mun_id from estaciones_NOAA) as en using(noaa_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where tmin is not null 
group by date_format(fecha, '%Y-%m'), cpro) as noaa_t_min using (cpro, year_months)
left join (
select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(tmin), 2) as aemet_mean_tmin
from AEMET_observaciones as no
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as en using(aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where tmin is not null 
group by date_format(fecha, '%Y-%m'), cpro) as aemet_t_min using (cpro, year_months)
left join (
select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(prcp), 2) as noaa_mean_prcp
from NOAA_observaciones as no
left join (select noaa_id, ine_mun_id from estaciones_NOAA) as en using(noaa_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where prcp is not null 
group by date_format(fecha, '%Y-%m'), cpro) as noaa_prcp using (cpro, year_months)
left join (
select cpro, date_format(fecha, '%Y-%m') as year_months, round(avg(prec), 2) as aemet_mean_prcp
from AEMET_observaciones as no
left join (select aemet_id, ine_mun_id from estaciones_AEMET) as en using(aemet_id)
left join (select ine_mun_id, cpro from municipios) as m using (ine_mun_id)
where prec is not null 
group by date_format(fecha, '%Y-%m'), cpro) as aemet_prcp using (cpro, year_months);






    
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/comunidades_autonomas.csv'
into table comunidades_autonomas
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(codauto, comunidad_autonoma);

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/provincias.csv'
into table provincias
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(cpro, provincia);
    
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/MUNICIPIOS.csv'
into table MUNICIPIOS
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(ine_mun_id, cpro, codauto, nombre, capital, latitud, longitud, latitud_dec , longitud_dec , altitud, num_habitantes);
    
    

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/ZIPs.csv'
into table ZIPs
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(ine_mun_id, zip);


load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/PRO_GEOMETRY.csv'
into table PRO_GEOMETRY
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(cpro, pro_geometry);


load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/MUN_GEOMETRY.csv'
into table MUN_GEOMETRY
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(ine_mun_id, mun_geometry);


load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/total_municipios.csv'
into table total_municipios
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(codauto, cpro, ine_mun_id, comunidad_autonoma, provincia, nombre, capital, latitud, longitud, latitud_dec, longitud_dec, altitud, num_habitantes, ZIPs_array, mun_geometry);


load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/estaciones_AEMET.csv'
into table estaciones_AEMET
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(aemet_id, aemet_nombre, ine_mun_id, latitud, longitud, latitud_dec, longitud_dec, altitud);


load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/estaciones_NOAA.csv'
into table estaciones_NOAA
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(noaa_id, noaa_nombre, ine_mun_id, latitud_dec, longitud_dec, altitud);


load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/AEMET_observaciones.csv'
into table AEMET_observaciones
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(aemet_id, fecha, @tmed, @prec, @tmin, @horatmin, @tmax, @horatmax, @dir, @velmedia, @racha, @horaracha, @presmax, @horapresmax, @presmin, @horapresmin, @sol)
set tmed = nullif(@tmed, ''),
	prec = nullif(@prec, ''),
	tmin = nullif(@tmin, ''),
    horatmin = nullif(@horatmin, ''),
    tmax = nullif(@tmax, ''),
    horatmax = nullif(@horatmax, ''),
    dir = nullif(@dir, ''),
    velmedia = nullif(@velmedia, ''),
    racha = nullif(@racha, ''),
    horaracha = nullif(@horaracha, ''),
    presmax = nullif(@presmax, ''),
    horapresmax = nullif(@horapresmax, ''),
    presmin = nullif(@presmin, ''),
    horapresmin = nullif(@horapresmin, ''),
    sol = nullif(@sol, '');
    
    
load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TFM_observaciones_clima/Tablas_BBDD/NOAA_observaciones.csv'
into table NOAA_observaciones
character set UTF8
fields terminated by ';'
lines terminated by '\n'
ignore 1 rows
(noaa_id, fecha, @tmax, @tmin, @prcp, @tavg, @snwd)
set tmax = nullif(@tmax, ''),
	tmin = nullif(@tmin, ''),
	prcp = nullif(@prcp, ''),
    tavg = nullif(@tavg, ''),
    snwd = nullif(@snwd, '');
    


alter table PRO_GEOMETRY
add PRO_geometry_GEOM geometry;


update PRO_GEOMETRY 
SET PRO_geometry_GEOM = st_geomfromtext(pro_geometry)
where geo_pro_id >0;



alter table MUN_GEOMETRY
add MUN_geometry_GEOM geometry;


update MUN_GEOMETRY 
SET MUN_geometry_GEOM = st_geomfromtext(mun_geometry)
where geo_mun_id >0;




