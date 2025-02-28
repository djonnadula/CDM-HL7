
SELECT	   
fac.Facility_Mnemonic,
        facloc.Location_Mnemonic,
		 fac.Coid, 
       fac.Network_Mnemonic_CS,
	   fac.Facility_Desc
FROM	   EDWCL_VIEWS.Clinical_Facility fac
LEFT JOIN EDWCL_VIEWS.Clinical_Facility_Location facloc 
	ON facloc.COID = fac.COID 
WHERE	fac.Company_Code='H'
	AND fac.Facility_Active_Ind='Y'