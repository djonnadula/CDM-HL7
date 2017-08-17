SELECT
fac.Facility_Mnemonic,
        facloc.Location_Mnemonic,
		        fac.Coid,
				fac.Network_Mnemonic_CS
FROM    EDWCL_VIEWS.Clinical_Facility fac
INNER JOIN EDWCL_VIEWS.Clinical_Facility_Location facloc ON facloc.COID = fac.COID
WHERE fac.Company_Code='H'
AND fac.Facility_Active_Ind='Y'


