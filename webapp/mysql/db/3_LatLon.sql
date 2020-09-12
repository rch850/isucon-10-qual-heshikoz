update isuumo.estate set latlon=point(latitude,longitude);
alter table isuumo.estate modify column latlon geometry not null;
alter table isuumo.estate add SPATIAL INDEX index_latlon(latlon);

