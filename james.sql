-- viewer.opa__assessments
-- import.lni__li_residentialpermits
-- viewer.ais__address_summar
-- Join AIS, OPA, & LNI to find most recent assessed values and percentage taxable values of residences with 1 vs. 2+ additions
with opa as (
	select oa.parcel_number , max(oa."year") as year_assessed
	from viewer.opa__assessments oa
	group by oa.parcel_number
) , 
lni as (
	select llr.addresskey , count(llr.objectid) as addition_count
	from import.lni__li_residentialpermits llr 
	group by llr.addresskey , llr.aptype 
	having llr.aptype = 'BP_ADDITON'
) , 
list as (
	select aas.id , aas.address , aas.address_suffix , aas.address_suffix , aas.address_high , 
		aas.address_full , aas.street_predir , aas.street_name , aas.street_suffix , aas.street_postdir , 
		aas.unit_type , aas.unit_num , aas.zip_code , aas.zip_4 , aas.street_address , aas.seg_id , aas.seg_side , 
		aas.opa_account_num , 
		opa.year_assessed , 
		oa2.market_value , oa2.taxable_land , oa2.taxable_building , oa2.exempt_land , oa2.exempt_building , 
		lni.addition_count
	from viewer.ais__address_summary aas 
	inner join opa 
		on aas.opa_account_num = opa.parcel_number 
	inner join viewer.opa__assessments oa2 
		on opa.parcel_number = oa2.parcel_number and opa.year_assessed = oa2.year
	inner join lni
		on aas.li_address_key = lni.addresskey
	where taxable_land + taxable_building != 0
	group by aas.id , aas.address , aas.address_suffix , aas.address_suffix , aas.address_high , 
		aas.address_full , aas.street_predir , aas.street_name , aas.street_suffix , aas.street_postdir , 
		aas.unit_type , aas.unit_num , aas.zip_code , aas.zip_4 , aas.street_address , aas.seg_id , aas.seg_side , 
		aas.opa_account_num , 
		opa.year_assessed , 
		oa2.market_value , oa2.taxable_land , oa2.taxable_building , oa2.exempt_land , oa2.exempt_building , 
		lni.addition_count
)
select a.additions_group, count(a.id) as cnt , 
	round(avg(a.market_value), 0) as avg_market_value , 
	round(avg(a.taxable_land), 0) as avg_taxable_land , round(avg(a.taxable_land) / avg(a.market_value), 3) as perc_taxable_land , 
	round(avg(a.taxable_building), 0) as avg_taxable_building , round(avg(a.taxable_building) / avg(a.market_value), 3) as perc_taxable_building 
from (  
	select * , 
		case 
			when list.addition_count = 1 then '1'
			when list.addition_count > 1 then '2+'
		end as additions_group
	from list) as a
group by a.additions_group;
--\q
