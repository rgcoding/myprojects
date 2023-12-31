item_cols = ['RECORDNO', 'ITEMID', 'STATUS', 'MRR', 'NAME', 'EXTENDED_DESCRIPTION', 'PODESCRIPTION',
        'SODESCRIPTION', 'PRODUCTLINEID', 'CYCLE', 'CNBILLINGTEMPLKEY', 'CNREVENUETEMPLKEY',
        'CNREVENUE2TEMPLKEY', 'CNEXPENSETEMPLKEY', 'CNEXPENSE2TEMPLKEY', 'CNBILLINGTEMPLATENAME',
        'CNREVENUETEMPLATENAME', 'CNREVENUE2TEMPLATENAME', 'CNEXPENSETEMPLATENAME', 'CNEXPENSE2TEMPLATENAME',
        'CNDEFAULTBUNDLE', 'CNMEACATEGORYKEY', 'CNMEACATEGORYNAME', 'PRODUCTTYPE', 'SUBSTITUTEID',
        'SHIP_WEIGHT', 'WHENLASTSOLD', 'WHENLASTRECEIVED', 'INCOMEACCTKEY', 'EXPENSEACCTKEY', 'INVACCTKEY',
        'COGSACCTKEY', 'OFFSETOEGLACCOUNTKEY', 'OFFSETPOGLACCOUNTKEY', 'DEFERREDREVACCTKEY',
        'DEFAULTREVRECTEMPLKEY', 'ALLOW_BACKORDER', 'TAXABLE', 'TAXGROUP.NAME', 'TAXCODE', 'COST_METHOD',
        'STANDARD_COST', 'UOMGRP', 'BASEUOM', 'UOM.INVUOMDETAIL.UNIT', 'UOM.POUOMDETAIL.UNIT',
        'UOM.POUOMDETAIL.CONVFACTOR', 'UOM.SOUOMDETAIL.UNIT', 'UOM.SOUOMDETAIL.CONVFACTOR',
        'DEFAULT_WAREHOUSE', 'GLGROUP', 'NOTE', 'INV_PRECISION', 'PO_PRECISION', 'SO_PRECISION',
        'ITEMTYPE', 'ENABLE_SERIALNO', 'SERIAL_MASKKEY', 'ENABLE_LOT_CATEGORY', 'LOT_CATEGORYKEY',
        'ENABLE_BINS', 'ENABLE_EXPIRATION', 'UPC', 'REVPOSTING', 'REVPRINTING', 'VSOECATEGORY',
        'VSOEDLVRSTATUS', 'VSOEREVDEFSTATUS', 'HASSTARTENDDATES', 'TERMPERIOD', 'TOTALPERIODS',
        'COMPUTEFORSHORTTERM', 'RENEWALMACROID', 'RENEWALMACROKEY', 'ITEMGLGROUP.INCOMEACCOUNT.ACCTNO',
        'ITEMGLGROUP.EXPENSEACCOUNT.ACCTNO', 'ITEMGLGROUP.INVENTORYACCOUNT.ACCTNO',
        'ITEMGLGROUP.COGSACCOUNT.ACCTNO', 'ITEMGLGROUP.DEFRRGLACCOUNT.ACCTNO',
        'ITEMGLGROUP.OEGLACCOUNT.ACCTNO', 'ITEMGLGROUP.POGLACCOUNT.ACCTNO', 'ENABLELANDEDCOST',
        'DEFCONTRACTDELIVERYSTATUS', 'DEFCONTRACTDEFERRALSTATUS', 'WHENCREATED', 'WHENMODIFIED', 'CREATEDBY',
        'MODIFIEDBY', 'IONORDER', 'IONHAND', 'IONHOLD', 'GLGRPKEY', 'UOMGRPKEY', 'TAXGROUPKEY', 'DROPSHIP',
        'BUYTOORDER', 'BASEPRICE', 'AUTOPRINTLABEL', 'MEGAENTITYKEY', 'MEGAENTITYID', 'MEGAENTITYNAME',
        'FIXED_ASSET']


gld_cols = ['RECORDNO', 'BATCH_DATE', 'BATCH_TITLE', 'SYMBOL', 'BATCH_NO', 'BOOKID', 'CHILDENTITY',
            'MODIFIED', 'REFERENCENO', 'ADJ', 'MODULEKEY', 'LINE_NO', 'ENTRY_DATE', 'TR_TYPE',
            'DOCUMENT', 'ACCOUNTNO', 'ACCOUNTTITLE', 'STATISTICAL', 'DEPARTMENTID', 'DEPARTMENTTITLE', 'LOCATIONID',
            'LOCATIONNAME', 'CURRENCY', 'BASECURR', 'DESCRIPTION', 'DEBITAMOUNT', 'CREDITAMOUNT', 'AMOUNT',
            'TRX_DEBITAMOUNT', 'TRX_CREDITAMOUNT', 'TRX_AMOUNT', 'CLEARED', 'CLRDATE', 'CUSTENTITY', 'VENDENTITY',
            'EMPENTITY', 'LOCENTITY', 'RECORDTYPE', 'RECORDID', 'DOCNUMBER', 'STATE', 'WHENCREATED', 'WHENDUE',
            'WHENPAID', 'WHENMODIFIED', 'PRDESCRIPTION', 'PRCLEARED', 'PRCLRDATE', 'FINANCIALENTITY', 'TOTALENTERED',
            'TOTALPAID', 'TOTALDUE', 'ENTRYDESCRIPTION', 'GLENTRYKEY', 'CREATEDBY', 'BATCH_STATE', 'ENTRY_STATE',
             'PROJECTID', 'PROJECTNAME','CUSTOMERID', 'CUSTOMERNAME', 'VENDORID', 'VENDORNAME','EMPLOYEEID',
            'EMPLOYEENAME', 'ITEMID', 'ITEMNAME', 'CLASSID', 'CLASSNAME', 'RECORD_URL']

glaccount_cols = ['RECORDNO', 'ACCOUNTNO', 'TITLE', 'ACCOUNTTYPE', 'NORMALBALANCE', 'CLOSINGTYPE',
                  'CLOSINGACCOUNTNO', 'CLOSINGACCOUNTTITLE', 'STATUS', 'REQUIREDEPT', 'REQUIRELOC',
                  'TAXABLE', 'CATEGORYKEY', 'CATEGORY', 'TAXCODE', 'MRCCODE', 'CLOSETOACCTKEY',
                  'ALTERNATIVEACCOUNT', 'WHENCREATED', 'WHENMODIFIED', 'CREATEDBY', 'MODIFIEDBY',
                  'SUBLEDGERCONTROLON', 'MEGAENTITYKEY', 'MEGAENTITYID', 'MEGAENTITYNAME', 'FA_ENTITIES_ALLOWED',
                  'FIXED_ASSET_CLEARING_ACCOUNT', 'REQUIREPROJECT', 'REQUIRECUSTOMER', 'REQUIREVENDOR',
                  'REQUIREEMPLOYEE', 'REQUIREITEM', 'REQUIRECLASS', 'REQUIREGLDIMPROCESS',
                  'REQUIREGLDIMBUSINESS_UNIT', 'RBUSINESS_UNIT']

class_cols = ['RECORDNO', 'CLASSID', 'NAME', 'DESCRIPTION', 'STATUS', 'PARENTKEY', 'PARENTID', 'PARENTNAME',
              'WHENCREATED', 'WHENMODIFIED', 'CREATEDBY', 'MODIFIEDBY', 'MEGAENTITYKEY', 'MEGAENTITYID',
              'MEGAENTITYNAME', 'RLOCATION']

customer_cols = ['RECORDNO', 'CUSTOMERID', 'NAME', 'ENTITY', 'PARENTKEY', 'PARENTID', 'PARENTNAME',
                 'DISPLAYCONTACT.CONTACTNAME', 'DISPLAYCONTACT.COMPANYNAME', 'DISPLAYCONTACT.PREFIX',
                 'DISPLAYCONTACT.FIRSTNAME', 'DISPLAYCONTACT.LASTNAME', 'DISPLAYCONTACT.INITIAL',
                 'DISPLAYCONTACT.PRINTAS', 'DISPLAYCONTACT.TAXABLE', 'DISPLAYCONTACT.TAXGROUP',
                 'DISPLAYCONTACT.TAXID', 'DISPLAYCONTACT.PHONE1', 'DISPLAYCONTACT.PHONE2', 'DISPLAYCONTACT.CELLPHONE',
                 'DISPLAYCONTACT.PAGER', 'DISPLAYCONTACT.FAX', 'DISPLAYCONTACT.TAXIDVALIDATIONDATE',
                 'DISPLAYCONTACT.TAXCOMPANYNAME', 'DISPLAYCONTACT.TAXADDRESS', 'DISPLAYCONTACT.EMAIL1',
                 'DISPLAYCONTACT.EMAIL2', 'DISPLAYCONTACT.URL1', 'DISPLAYCONTACT.URL2', 'DISPLAYCONTACT.VISIBLE',
                 'DISPLAYCONTACT.MAILADDRESS.ADDRESS1', 'DISPLAYCONTACT.MAILADDRESS.ADDRESS2',
                 'DISPLAYCONTACT.MAILADDRESS.CITY', 'DISPLAYCONTACT.MAILADDRESS.STATE',
                 'DISPLAYCONTACT.MAILADDRESS.ZIP', 'DISPLAYCONTACT.MAILADDRESS.COUNTRY',
                 'DISPLAYCONTACT.MAILADDRESS.COUNTRYCODE', 'DISPLAYCONTACT.MAILADDRESS.LATITUDE',
                 'DISPLAYCONTACT.MAILADDRESS.LONGITUDE', 'DISPLAYCONTACT.STATUS', 'TERMNAME', 'TERMVALUE',
                 'CUSTREPID', 'CUSTREPNAME', 'RESALENO', 'TAXID', 'CREDITLIMIT', 'TOTALDUE', 'COMMENTS',
                 'ACCOUNTLABEL', 'ARACCOUNT', 'ARACCOUNTTITLE', 'LAST_INVOICEDATE', 'LAST_STATEMENTDATE',
                 'DELIVERY_OPTIONS', 'TERRITORYID', 'SHIPPINGMETHOD', 'CUSTTYPE', 'GLGRPKEY', 'GLGROUP',
                 'PRICESCHEDULE', 'DISCOUNT', 'PRICELIST', 'VSOEPRICELIST', 'CURRENCY', 'CONTACTINFO.CONTACTNAME',
                 'CONTACTINFO.PREFIX', 'CONTACTINFO.FIRSTNAME', 'CONTACTINFO.INITIAL', 'CONTACTINFO.LASTNAME',
                 'CONTACTINFO.COMPANYNAME', 'CONTACTINFO.PRINTAS', 'CONTACTINFO.PHONE1', 'CONTACTINFO.PHONE2',
                 'CONTACTINFO.CELLPHONE', 'CONTACTINFO.PAGER', 'CONTACTINFO.FAX', 'CONTACTINFO.EMAIL1',
                 'CONTACTINFO.EMAIL2', 'CONTACTINFO.URL1', 'CONTACTINFO.URL2', 'CONTACTINFO.VISIBLE',
                 'CONTACTINFO.MAILADDRESS.ADDRESS1', 'CONTACTINFO.MAILADDRESS.ADDRESS2',
                 'CONTACTINFO.MAILADDRESS.CITY', 'CONTACTINFO.MAILADDRESS.STATE', 'CONTACTINFO.MAILADDRESS.ZIP',
                 'CONTACTINFO.MAILADDRESS.COUNTRY', 'CONTACTINFO.MAILADDRESS.COUNTRYCODE', 'SHIPTO.CONTACTNAME',
                 'SHIPTO.PREFIX', 'SHIPTO.FIRSTNAME', 'SHIPTO.INITIAL', 'SHIPTO.LASTNAME', 'SHIPTO.COMPANYNAME',
                 'SHIPTO.PRINTAS', 'SHIPTO.TAXABLE', 'SHIPTO.TAXGROUP', 'SHIPTO.TAXID', 'SHIPTO.PHONE1',
                 'SHIPTO.PHONE2', 'SHIPTO.CELLPHONE', 'SHIPTO.PAGER', 'SHIPTO.FAX', 'SHIPTO.EMAIL1', 'SHIPTO.EMAIL2',
                 'SHIPTO.URL1', 'SHIPTO.URL2', 'SHIPTO.VISIBLE', 'SHIPTO.MAILADDRESS.ADDRESS1',
                 'SHIPTO.MAILADDRESS.ADDRESS2', 'SHIPTO.MAILADDRESS.CITY', 'SHIPTO.MAILADDRESS.STATE',
                 'SHIPTO.MAILADDRESS.ZIP', 'SHIPTO.MAILADDRESS.COUNTRY', 'SHIPTO.MAILADDRESS.COUNTRYCODE',
                 'BILLTO.CONTACTNAME', 'BILLTO.PREFIX', 'BILLTO.FIRSTNAME', 'BILLTO.INITIAL', 'BILLTO.LASTNAME',
                 'BILLTO.COMPANYNAME', 'BILLTO.PRINTAS', 'BILLTO.TAXABLE', 'BILLTO.TAXGROUP', 'BILLTO.PHONE1',
                 'BILLTO.PHONE2', 'BILLTO.CELLPHONE', 'BILLTO.PAGER', 'BILLTO.FAX', 'BILLTO.EMAIL1', 'BILLTO.EMAIL2',
                 'BILLTO.URL1', 'BILLTO.URL2', 'BILLTO.VISIBLE', 'BILLTO.MAILADDRESS.ADDRESS1',
                 'BILLTO.MAILADDRESS.ADDRESS2', 'BILLTO.MAILADDRESS.CITY', 'BILLTO.MAILADDRESS.STATE',
                 'BILLTO.MAILADDRESS.ZIP', 'BILLTO.MAILADDRESS.COUNTRY', 'BILLTO.MAILADDRESS.COUNTRYCODE',
                 'STATUS', 'ONETIME', 'CUSTMESSAGEID', 'ONHOLD', 'PRCLST_OVERRIDE', 'OEPRCLSTKEY', 'OEPRICESCHEDKEY',
                 'ENABLEONLINECARDPAYMENT', 'ENABLEONLINEACHPAYMENT', 'VSOEPRCLSTKEY', 'WHENMODIFIED',
                 'ARINVOICEPRINTTEMPLATEID', 'OEQUOTEPRINTTEMPLATEID', 'OEORDERPRINTTEMPLATEID',
                 'OELISTPRINTTEMPLATEID', 'OEINVOICEPRINTTEMPLATEID', 'OEADJPRINTTEMPLATEID',
                 'OEOTHERPRINTTEMPLATEID', 'WHENCREATED', 'CREATEDBY', 'MODIFIEDBY', 'DISPLAYCONTACTKEY',
                 'CONTACTKEY', 'SHIPTOKEY', 'BILLTOKEY', 'CUSTREPKEY', 'SHIPVIAKEY', 'TERRITORYKEY', 'TERMSKEY',
                 'ACCOUNTLABELKEY', 'ACCOUNTKEY', 'CUSTTYPEKEY', 'PRICESCHEDULEKEY', 'OFFSETGLACCOUNTNO',
                 'OFFSETGLACCOUNTNOTITLE', 'ADVBILLBY', 'ADVBILLBYTYPE', 'SUPDOCID', 'RETAINAGEPERCENTAGE',
                 'MEGAENTITYKEY', 'MEGAENTITYID', 'MEGAENTITYNAME', 'LICENSE_NUMBER', 'LICENSE_EXP_DATE',
                 'HIDEDISPLAYCONTACT']

vendor_cols = ['RECORDNO', 'VENDORID', 'NAME', 'NAME1099', 'PARENTKEY', 'PARENTID', 'PARENTNAME',
               'DISPLAYCONTACT.CONTACTNAME', 'DISPLAYCONTACT.COMPANYNAME', 'DISPLAYCONTACT.PREFIX',
               'DISPLAYCONTACT.FIRSTNAME', 'DISPLAYCONTACT.LASTNAME', 'DISPLAYCONTACT.INITIAL',
               'DISPLAYCONTACT.PRINTAS', 'DISPLAYCONTACT.TAXABLE', 'DISPLAYCONTACT.TAXGROUP',
               'DISPLAYCONTACT.TAXID', 'DISPLAYCONTACT.PHONE1', 'DISPLAYCONTACT.PHONE2', 'DISPLAYCONTACT.CELLPHONE',
               'DISPLAYCONTACT.PAGER', 'DISPLAYCONTACT.FAX', 'DISPLAYCONTACT.TAXIDVALIDATIONDATE',
               'DISPLAYCONTACT.TAXCOMPANYNAME', 'DISPLAYCONTACT.TAXADDRESS', 'DISPLAYCONTACT.EMAIL1',
               'DISPLAYCONTACT.EMAIL2', 'DISPLAYCONTACT.URL1', 'DISPLAYCONTACT.URL2', 'DISPLAYCONTACT.VISIBLE',
               'DISPLAYCONTACT.MAILADDRESS.ADDRESS1', 'DISPLAYCONTACT.MAILADDRESS.ADDRESS2',
               'DISPLAYCONTACT.MAILADDRESS.CITY', 'DISPLAYCONTACT.MAILADDRESS.STATE',
               'DISPLAYCONTACT.MAILADDRESS.ZIP', 'DISPLAYCONTACT.MAILADDRESS.COUNTRY',
               'DISPLAYCONTACT.MAILADDRESS.COUNTRYCODE', 'DISPLAYCONTACT.MAILADDRESS.LATITUDE',
               'DISPLAYCONTACT.MAILADDRESS.LONGITUDE', 'DISPLAYCONTACT.STATUS', 'ENTITY', 'TERMNAME',
               'TERMVALUE', 'VENDORACCOUNTNO', 'TAXID', 'CREDITLIMIT', 'TOTALDUE', 'BILLINGTYPE', 'VENDTYPE',
               'VENDTYPE1099TYPE', 'GLGROUP', 'PRICESCHEDULE', 'DISCOUNT', 'PRICELIST', 'COMMENTS', 'ACCOUNTLABEL',
               'APACCOUNT', 'APACCOUNTTITLE', 'FORM1099TYPE', 'FORM1099BOX', 'PAYMENTPRIORITY',
               'CONTACTINFO.CONTACTNAME', 'CONTACTINFO.PREFIX', 'CONTACTINFO.FIRSTNAME', 'CONTACTINFO.INITIAL',
               'CONTACTINFO.LASTNAME', 'CONTACTINFO.COMPANYNAME', 'CONTACTINFO.PRINTAS', 'CONTACTINFO.PHONE1',
               'CONTACTINFO.PHONE2', 'CONTACTINFO.CELLPHONE', 'CONTACTINFO.PAGER', 'CONTACTINFO.FAX',
               'CONTACTINFO.EMAIL1', 'CONTACTINFO.EMAIL2', 'CONTACTINFO.URL1', 'CONTACTINFO.URL2',
               'CONTACTINFO.VISIBLE', 'CONTACTINFO.MAILADDRESS.ADDRESS1', 'CONTACTINFO.MAILADDRESS.ADDRESS2',
               'CONTACTINFO.MAILADDRESS.CITY', 'CONTACTINFO.MAILADDRESS.STATE', 'CONTACTINFO.MAILADDRESS.ZIP',
               'CONTACTINFO.MAILADDRESS.COUNTRY', 'CONTACTINFO.MAILADDRESS.COUNTRYCODE', 'PAYTO.CONTACTNAME',
               'PAYTO.PREFIX', 'PAYTO.FIRSTNAME', 'PAYTO.INITIAL', 'PAYTO.LASTNAME', 'PAYTO.COMPANYNAME',
               'PAYTO.PRINTAS', 'PAYTO.PHONE1', 'PAYTO.PHONE2', 'PAYTO.CELLPHONE', 'PAYTO.PAGER', 'PAYTO.FAX',
               'PAYTO.EMAIL1', 'PAYTO.EMAIL2', 'PAYTO.URL1', 'PAYTO.URL2', 'PAYTO.VISIBLE',
               'PAYTO.MAILADDRESS.ADDRESS1', 'PAYTO.MAILADDRESS.ADDRESS2', 'PAYTO.MAILADDRESS.CITY',
               'PAYTO.MAILADDRESS.STATE', 'PAYTO.MAILADDRESS.ZIP', 'PAYTO.MAILADDRESS.COUNTRY',
               'PAYTO.MAILADDRESS.COUNTRYCODE', 'PAYTO.TAXGROUP', 'PAYTO.TAXID', 'RETURNTO.CONTACTNAME',
               'RETURNTO.PREFIX', 'RETURNTO.FIRSTNAME', 'RETURNTO.INITIAL', 'RETURNTO.LASTNAME',
               'RETURNTO.COMPANYNAME', 'RETURNTO.PRINTAS', 'RETURNTO.PHONE1', 'RETURNTO.PHONE2',
               'RETURNTO.CELLPHONE', 'RETURNTO.PAGER', 'RETURNTO.FAX', 'RETURNTO.EMAIL1', 'RETURNTO.EMAIL2',
               'RETURNTO.URL1', 'RETURNTO.URL2', 'RETURNTO.VISIBLE', 'RETURNTO.MAILADDRESS.ADDRESS1',
               'RETURNTO.MAILADDRESS.ADDRESS2', 'RETURNTO.MAILADDRESS.CITY', 'RETURNTO.MAILADDRESS.STATE',
               'RETURNTO.MAILADDRESS.ZIP', 'RETURNTO.MAILADDRESS.COUNTRY', 'RETURNTO.MAILADDRESS.COUNTRYCODE',
               'STATUS', 'PAYDATEVALUE', 'ONETIME', 'ONHOLD', 'WHENMODIFIED', 'ISOWNER', 'DONOTCUTCHECK',
               'OWNER.EQGLACCOUNT', 'OWNER.EQGLACCOUNTLABEL', 'OWNER.HOLDDIST', 'OWNER.ACCOUNTLABEL.LABEL',
               'CURRENCY', 'FILEPAYMENTSERVICE', 'ACHENABLED', 'WIREENABLED', 'CHECKENABLED', 'ACHBANKROUTINGNUMBER',
               'ACHACCOUNTNUMBER', 'ACHACCOUNTTYPE', 'ACHREMITTANCETYPE', 'WIREBANKNAME', 'WIREBANKROUTINGNUMBER',
               'WIREACCOUNTNUMBER', 'WIREACCOUNTTYPE', 'PMPLUSREMITTANCETYPE', 'PMPLUSEMAIL', 'PMPLUSFAX',
               'DISPLAYTERMDISCOUNT', 'OEPRCLSTKEY', 'DISPLOCACCTNOCHECK', 'WHENCREATED', 'CREATEDBY', 'MODIFIEDBY',
               'PAYMENTNOTIFY', 'PAYMETHODKEY', 'MERGEPAYMENTREQ', 'DISPLAYCONTACTKEY', 'PRIMARYCONTACTKEY',
               'PAYTOKEY', 'RETURNTOKEY', 'ACCOUNTLABELKEY', 'ACCOUNTKEY', 'VENDTYPEKEY', 'GLGRPKEY', 'TERMSKEY',
               'VENDORACCTNOKEY', 'PAYMETHODREC', 'OUTSOURCECHECK', 'OUTSOURCECHECKSTATE', 'OUTSOURCEACH',
               'OUTSOURCEACHSTATE', 'OUTSOURCECARD', 'OUTSOURCECARDOVERRIDE', 'CARDSTATE', 'VENDORACHACCOUNTID',
               'VENDORACCOUNTOUTSOURCEACH', 'OFFSETGLACCOUNTNO', 'OFFSETGLACCOUNTNOTITLE',
               'CONTACTTO1099.CONTACTNAME', 'CONTACTTO1099.PREFIX', 'CONTACTTO1099.FIRSTNAME',
               'CONTACTTO1099.INITIAL', 'CONTACTTO1099.LASTNAME', 'CONTACTTO1099.COMPANYNAME',
               'CONTACTTO1099.PRINTAS', 'CONTACTTO1099.PHONE1', 'CONTACTTO1099.PHONE2', 'CONTACTTO1099.CELLPHONE',
               'CONTACTTO1099.PAGER', 'CONTACTTO1099.FAX', 'CONTACTTO1099.EMAIL1', 'CONTACTTO1099.EMAIL2',
               'CONTACTTO1099.URL1', 'CONTACTTO1099.URL2', 'CONTACTTO1099.VISIBLE',
               'CONTACTTO1099.MAILADDRESS.ADDRESS1', 'CONTACTTO1099.MAILADDRESS.ADDRESS2',
               'CONTACTTO1099.MAILADDRESS.CITY', 'CONTACTTO1099.MAILADDRESS.STATE',
               'CONTACTTO1099.MAILADDRESS.ZIP', 'CONTACTTO1099.MAILADDRESS.COUNTRY',
               'CONTACTTO1099.MAILADDRESS.COUNTRYCODE', 'CONTACTKEY1099', 'SUPDOCID', 'VENDOR_AMEX_ORGANIZATION_ID',
               'VENDOR_AMEX_ORG_ADDRESS_ID', 'VENDOR_AMEX_CD_AFFILIATE_ID', 'VENDOR_AMEX_CARD_AFFILIATE_ID',
               'AMEX_BANK_ACCOUNT_ID', 'AMEX_BANK_ACCOUNT_ADDRESS_ID', 'RETAINAGEPERCENTAGE', 'MEGAENTITYKEY',
               'MEGAENTITYID', 'MEGAENTITYNAME', 'HIDEDISPLAYCONTACT']

contact_cols = ['RECORDNO', 'CONTACTNAME', 'COMPANYNAME', 'PREFIX', 'FIRSTNAME', 'LASTNAME', 'INITIAL', 'PRINTAS',
                'TAXABLE', 'TAXGROUP', 'PHONE1', 'PHONE2', 'CELLPHONE', 'PAGER', 'FAX', 'EMAIL1', 'EMAIL2', 'URL1',
                'URL2', 'VISIBLE', 'MAILADDRESS.ADDRESS1', 'MAILADDRESS.ADDRESS2', 'MAILADDRESS.CITY',
                'MAILADDRESS.STATE', 'MAILADDRESS.ZIP', 'MAILADDRESS.COUNTRY', 'STATUS', 'MAILADDRESS.COUNTRYCODE',
                'MAILADDRESS.LATITUDE', 'MAILADDRESS.LONGITUDE', 'PRICESCHEDULE', 'DISCOUNT', 'PRICELIST',
                'PRICELISTKEY', 'TAXID', 'TAXGROUPKEY', 'PRICESCHEDULEKEY', 'WHENCREATED', 'WHENMODIFIED',
                'CREATEDBY', 'MODIFIEDBY', 'MEGAENTITYKEY', 'MEGAENTITYID', 'MEGAENTITYNAME']

location_cols = ['LOCATIONID', 'RECORDNO', 'NAME', 'PARENTID', 'SUPERVISORNAME', 'SUPERVISORID',
                  'CONTACTINFO.CONTACTNAME', 'CONTACTINFO.PRINTAS', 'CONTACTINFO.PHONE1', 'CONTACTINFO.PHONE2',
                  'CONTACTINFO.EMAIL1', 'CONTACTINFO.EMAIL2', 'CONTACTINFO.FAX', 'CONTACTINFO.MAILADDRESS.ADDRESS1',
                  'CONTACTINFO.MAILADDRESS.ADDRESS2', 'CONTACTINFO.MAILADDRESS.CITY', 'CONTACTINFO.MAILADDRESS.STATE',
                  'CONTACTINFO.MAILADDRESS.ZIP', 'CONTACTINFO.MAILADDRESS.COUNTRY',
                  'CONTACTINFO.MAILADDRESS.COUNTRYCODE', 'STARTDATE', 'ENDDATE', 'SHIPTO.CONTACTNAME',
                  'SHIPTO.PHONE1', 'SHIPTO.PHONE2', 'SHIPTO.MAILADDRESS.ADDRESS1', 'SHIPTO.MAILADDRESS.ADDRESS2',
                  'SHIPTO.MAILADDRESS.CITY', 'SHIPTO.MAILADDRESS.STATE', 'SHIPTO.MAILADDRESS.ZIP',
                  'SHIPTO.MAILADDRESS.COUNTRY', 'SHIPTO.MAILADDRESS.COUNTRYCODE', 'STATUS', 'WHENCREATED',
                  'WHENMODIFIED', 'FEDERALID', 'FIRSTMONTH', 'WEEKSTART', 'IEPAYABLE.ACCOUNT', 'IEPAYABLE.NUMBER',
                  'IERECEIVABLE.ACCOUNT', 'IERECEIVABLE.NUMBER', 'MESSAGE_TEXT', 'MARKETING_TEXT', 'FOOTNOTETEXT',
                  'REPORTPRINTAS', 'ISROOT', 'RESERVEAMT', 'VENDORNAME', 'VENDORID', 'CUSTOMERID', 'CUSTOMERNAME',
                  'CURRENCY', 'ENTITY', 'ENTITYRECORDNO', 'HAS_IE_RELATION', 'CUSTTITLE', 'BUSINESSDAYS', 'WEEKENDS',
                  'FIRSTMONTHTAX', 'CONTACTKEY', 'SUPERVISORKEY', 'PARENTKEY', 'SHIPTOKEY', 'IEPAYABLEACCTKEY',
                  'IERECEIVABLEACCTKEY', 'VENDENTITY', 'CUSTENTITY', 'TAXID', 'CREATEDBY', 'MODIFIEDBY']

department_cols = ['DEPARTMENTID', 'RECORDNO', 'TITLE', 'PARENTKEY', 'PARENTID', 'SUPERVISORKEY', 'SUPERVISORID',
                   'WHENCREATED', 'WHENMODIFIED', 'SUPERVISORNAME', 'STATUS', 'CUSTTITLE', 'CREATEDBY', 'MODIFIEDBY']

sodocumentary_cols = ['RECORDNO', 'DOCHDRNO', 'DOCHDRID', 'SALE_PUR_TRANS', 'BUNDLENUMBER', 'LINE_NO', 'ITEMID',
                      'ITEMNAME', 'ITEMDESC', 'UNIT', 'WAREHOUSE.LOCATION_NO', 'WAREHOUSE.NAME', 'MEMO',
                      'PRICECALCMEMO', 'QUANTITY', 'QTY_CONVERTED', 'RETAILPRICE', 'PRICE', 'TOTAL', 'WHENCREATED',
                      'WHENMODIFIED', 'AUWHENCREATED', 'CREATEDBY', 'MODIFIEDBY', 'ITEM.TAXABLE',
                      'ITEM.TAXGROUP.RECORDNO', 'ITEM.RENEWALMACRO.MACROID', 'EXTENDED_DESCRIPTION', 'ITEMGLGROUP',
                      'ITEMGLGROUPKEY', 'ITEMGLGROUPNAME', 'STATE', 'STATUS', 'COST', 'COST_METHOD', 'UIQTY',
                      'DISCOUNTPERCENT', 'MULTIPLIER', 'UIPRICE', 'UIVALUE', 'LOCATIONID', 'LOCATIONNAME',
                      'DEPARTMENTID', 'DEPARTMENTNAME', 'DEPTKEY', 'LOCATIONKEY', 'TIMETYPEKEY', 'TIMETYPENAME',
                      'TIMENOTES', 'EEACCOUNTLABELKEY', 'EEACCOUNTLABEL', 'SOURCE_DOCKEY', 'SOURCE_DOCLINEKEY',
                      'ADJDOCHDRKEY', 'ADJDOCENTRYKEY', 'REVRECTEMPLATE', 'REVRECTEMPLATEKEY', 'REVRECSTARTDATE',
                      'ITEMTERM', 'TERMPERIOD', 'REVRECENDDATE', 'PRORATEPRICE', 'DEFERREVENUE', 'SC_REVRECTEMPLATE',
                      'SC_REVRECTEMPLATEKEY', 'SC_REVRECSTARTDATE', 'SC_REVRECENDDATE', 'SC_STARTDATE',
                      'ITEM.ITEMTYPE', 'ITEM.NUMDEC_SALE', 'ITEM.NUMDEC_STD', 'ITEM.NUMDEC_PUR', 'ITEM.REVPOSTING',
                      'ITEM.COMPUTEFORSHORTTERM', 'ITEM.RENEWALMACROKEY', 'ITEM.UOMGRPKEY', 'ITEM.DROPSHIP',
                      'ITEM.BUYTOORDER', 'DISCOUNT_MEMO', 'ITEM.REVPRINTING', 'CURRENCY', 'BASECURR', 'EXCHRATEDATE',
                      'EXCHRATETYPE', 'EXCHRATE', 'TRX_PRICE', 'TRX_VALUE', 'SCHEDULENAME', 'SCHEDULEID',
                      'RECURDOCNAME', 'RECURDOCID', 'RECURDOCENTRYKEY', 'RENEWALMACRO', 'RENEWALMACROKEY',
                      'OVERRIDETAX', 'SC_CREATERECURSCHED', 'SC_EXISTINGSCHED', 'SC_EXTENDLINEPERIOD',
                      'SC_INSTALLPRICING', 'RECURCONTRACTID', 'SOURCE_DOCID', 'BILLABLE', 'BILLED',
                      'BILLABLETIMEENTRYKEY', 'BILLABLEGLENTRYKEY', 'BILLABLEPRENTRYKEY', 'BILLABLEDOCENTRYKEY',
                      'BILLABLECONTRACTSCHENTRYKEY', 'BILLABLECONTRACTUSAGEBILLINGID', 'FORM1099', 'PERCENTVAL',
                      'TAXABSVAL', 'TAXABLEAMOUNT', 'LINETOTAL', 'DISCOUNT', 'TRX_TAXABSVAL', 'TRX_LINETOTAL',
                      'TAXVALOVERRIDE', 'FORM1099TYPE', 'FORM1099BOX', 'TOTAL_AMOUNT_CONVERTED',
                      'TOTAL_AMOUNT_REMAINING', 'QTY_REMAINING', 'PROJECTKEY', 'PROJECTNAME', 'TASKKEY', 'TASKID',
                      'TASKNAME', 'BILLINGTEMPLATEKEY', 'BILLINGTEMPLATE', 'BILLINGSCHEDULEKEY',
                      'BILLINGSCHEDULEENTRY.PERCENTCOMPLETED', 'BILLINGSCHEDULEENTRY.PERCENTBILLED',
                      'BILLINGSCHEDULEENTRY.BILLEDQTY', 'BILLINGSCHEDULEENTRY.ESTQTY',
                      'BILLINGSCHEDULEENTRY.TRUNCPERCENTCOMPLETED', 'BILLINGSCHEDULEENTRY.BILLINGTEMPLATEENTRYKEY',
                      'BILLINGSCHEDULEENTRY.BILLINGSCHEDULEKEY', 'GENINVOICELINEKEY', 'LINELEVELSIMPLETAXTYPE',
                      'NEEDBYDATE', 'SHIPBY', 'DONOTSHIPBEFOREDATE', 'DONOTSHIPAFTERDATE', 'DATEPICKTICKETPRINTED',
                      'CANCELAFTERDATE', 'SHIPTOKEY', 'SHIPTO.CONTACTNAME', 'SHIPTO.PREFIX', 'SHIPTO.FIRSTNAME',
                      'SHIPTO.INITIAL', 'SHIPTO.LASTNAME', 'SHIPTO.COMPANYNAME', 'SHIPTO.PRINTAS', 'SHIPTO.PHONE1',
                      'SHIPTO.PHONE2', 'SHIPTO.CELLPHONE', 'SHIPTO.PAGER', 'SHIPTO.FAX', 'SHIPTO.EMAIL1',
                      'SHIPTO.EMAIL2', 'SHIPTO.URL1', 'SHIPTO.URL2', 'SHIPTO.VISIBLE', 'SHIPTO.MAILADDRESS.ADDRESS1',
                      'SHIPTO.MAILADDRESS.ADDRESS2', 'SHIPTO.MAILADDRESS.CITY', 'SHIPTO.MAILADDRESS.STATE',
                      'SHIPTO.MAILADDRESS.ZIP', 'SHIPTO.MAILADDRESS.COUNTRY', 'SHIPTO.MAILADDRESS.COUNTRYCODE',
                      'BTOSHIPTOKEY', 'BTOSHIPTOCONTACTNAME', 'RETAINAGEPERCENTAGE', 'AMOUNTRETAINED',
                      'TRX_AMOUNTRETAINED', 'PROJECTDIMKEY', 'PROJECTID', 'CUSTOMERDIMKEY', 'CUSTOMERID',
                      'CUSTOMERNAME', 'VENDORDIMKEY', 'VENDORID', 'VENDORNAME', 'EMPLOYEEDIMKEY', 'EMPLOYEEID',
                      'EMPLOYEENAME', 'CLASSDIMKEY', 'CLASSID', 'CLASSNAME', 'BILLED_TIMESHEETENTRY.ENTRYDATE',
                      'GLDIMPROCESS', 'GLDIMBUSINESS_UNIT']

sodocument_cols =['RECORDNO', 'DOCNO', 'DOCID', 'CREATEDFROM', 'STATE', 'CLOSED', 'WHENCREATED', 'AUWHENCREATED',
                  'CREATEDBY', 'MODIFIEDBY', 'WHENMODIFIED', 'WHENDUE', 'STATUS', 'PONUMBER', 'VENDORDOCNO',
                  'DOCPARID', 'DOCPARKEY', 'DOCPARCLASS', 'UPDATES_INV', 'TERM.NAME', 'NOTE', 'WAREHOUSE.LOCATIONID',
                  'SHIPVIA', 'USER', 'CREATEDUSER', 'USERID', 'CREATEDUSERID', 'CONTACT.CONTACTNAME',
                  'CONTACT.PREFIX', 'CONTACT.FIRSTNAME', 'CONTACT.INITIAL', 'CONTACT.LASTNAME', 'CONTACT.COMPANYNAME',
                  'CONTACT.PRINTAS', 'CONTACT.PHONE1', 'CONTACT.PHONE2', 'CONTACT.CELLPHONE', 'CONTACT.PAGER',
                  'CONTACT.FAX', 'CONTACT.EMAIL1', 'CONTACT.EMAIL2', 'CONTACT.URL1', 'CONTACT.URL2',
                  'CONTACT.VISIBLE', 'CONTACT.MAILADDRESS.ADDRESS1', 'CONTACT.MAILADDRESS.ADDRESS2',
                  'CONTACT.MAILADDRESS.CITY', 'CONTACT.MAILADDRESS.STATE', 'CONTACT.MAILADDRESS.ZIP',
                  'CONTACT.MAILADDRESS.COUNTRY', 'CONTACT.MAILADDRESS.COUNTRYCODE', 'SHIPTOKEY', 'SHIPTO.CONTACTNAME',
                  'SHIPTO.PREFIX', 'SHIPTO.FIRSTNAME', 'SHIPTO.INITIAL', 'SHIPTO.LASTNAME', 'SHIPTO.COMPANYNAME',
                  'SHIPTO.PRINTAS', 'SHIPTO.PHONE1', 'SHIPTO.PHONE2', 'SHIPTO.CELLPHONE', 'SHIPTO.PAGER',
                  'SHIPTO.FAX', 'SHIPTO.EMAIL1', 'SHIPTO.EMAIL2', 'SHIPTO.URL1', 'SHIPTO.URL2', 'SHIPTO.VISIBLE',
                  'SHIPTO.MAILADDRESS.ADDRESS1', 'SHIPTO.MAILADDRESS.ADDRESS2', 'SHIPTO.MAILADDRESS.CITY',
                  'SHIPTO.MAILADDRESS.STATE', 'SHIPTO.MAILADDRESS.ZIP', 'SHIPTO.MAILADDRESS.COUNTRY',
                  'SHIPTO.MAILADDRESS.COUNTRYCODE', 'BILLTOKEY', 'BILLTO.CONTACTNAME', 'BILLTO.PREFIX',
                  'BILLTO.FIRSTNAME', 'BILLTO.INITIAL', 'BILLTO.LASTNAME', 'BILLTO.COMPANYNAME', 'BILLTO.PRINTAS',
                  'BILLTO.PHONE1', 'BILLTO.PHONE2', 'BILLTO.CELLPHONE', 'BILLTO.PAGER', 'BILLTO.FAX', 'BILLTO.EMAIL1',
                  'BILLTO.EMAIL2', 'BILLTO.URL1', 'BILLTO.URL2', 'BILLTO.VISIBLE', 'BILLTO.MAILADDRESS.ADDRESS1',
                  'BILLTO.MAILADDRESS.ADDRESS2', 'BILLTO.MAILADDRESS.CITY', 'BILLTO.MAILADDRESS.STATE',
                  'BILLTO.MAILADDRESS.ZIP', 'BILLTO.MAILADDRESS.COUNTRY', 'BILLTO.MAILADDRESS.COUNTRYCODE',
                  'MESSAGE', 'PRRECORDKEY', 'INVBATCHKEY', 'PRINVBATCHKEY', 'ADDGLBATCHKEY', 'PRINTED', 'BACKORDER',
                  'SUBTOTAL', 'TOTAL', 'ENTGLGROUP', 'CURRENCY', 'EXCHRATEDATE', 'EXCHRATETYPES.NAME', 'EXCHRATE',
                  'SCHOPKEY', 'SALESCONTRACT', 'USEDASCONTRACT', 'TRX_SUBTOTAL', 'TRX_TOTAL', 'EXCH_RATE_TYPE_ID',
                  'RENEWEDDOC', 'BASECURR', 'SYSTEMGENERATED', 'INVOICERUNKEY', 'DOCPAR_IN_OUT', 'WHENPOSTED',
                  'PRINTEDUSERID', 'DATEPRINTED', 'PRINTEDBY', 'ADJ', 'TAXSOLUTIONKEY', 'TAXSOLUTIONID',
                  'SHOWMULTILINETAX', 'TAXMETHOD', 'CUSTREC', 'CUSTVENDID', 'CUSTVENDNAME', 'HASPOSTEDREVREC',
                  'CONTRACTID', 'CONTRACTDESC', 'TRX_TOTALPAID', 'TOTALPAID', 'TRX_TOTALENTERED', 'TOTALENTERED',
                  'TRX_TOTALDUE', 'TOTALDUE', 'PAYMENTSTATUS', 'SIGN_FLAG', 'VSOEPRICELIST', 'VSOEPRCLSTKEY',
                  'ORIGDOCDATE', 'HASADVBILLING', 'INVOICERUN_EXPENSEPRICEMARKUP', 'INVOICERUN_DESCRIPTION',
                  'PROJECTKEY', 'PROJECT', 'PROJECTNAME', 'CNCONTRACTKEY', 'CNCONTRACTID', 'CNCONTRACTNAME',
                  'NEEDBYDATE', 'CANCELAFTERDATE', 'DONOTSHIPBEFOREDATE', 'DONOTSHIPAFTERDATE', 'SERVICEDELIVERYDATE',
                  'TRACKINGNUMBER', 'CUSTOMERPONUMBER', 'RETAINAGEPERCENTAGE', 'SCOPE', 'INCLUSIONS', 'EXCLUSIONS',
                  'TERMS', 'SCHEDULESTARTDATE', 'ACTUALSTARTDATE', 'SCHEDULEDCOMPLETIONDATE', 'REVISEDCOMPLETIONDATE',
                  'SUBSTANTIALCOMPLETIONDATE', 'ACTUALCOMPLETIONDATE', 'NOTICETOPROCEED', 'RESPONSEDUE', 'EXECUTEDON',
                  'SCHEDULEIMPACT', 'INTERNALREFNO', 'INTERNALINITIATEDBYKEY', 'INTERNALINITIATEDBY',
                  'INTERNALINITIATEDBYNAME', 'INTERNALVERBALBYKEY', 'INTERNALVERBALBY', 'INTERNALVERBALBYNAME',
                  'INTERNALISSUEDBYKEY', 'INTERNALISSUEDBY', 'INTERNALISSUEDBYNAME', 'INTERNALISSUEDON',
                  'INTERNALAPPROVEDBYKEY', 'INTERNALAPPROVEDBY', 'INTERNALAPPROVEDBYNAME', 'INTERNALAPPROVEDON',
                  'INTERNALSIGNEDBYKEY', 'INTERNALSIGNEDBY', 'INTERNALSIGNEDBYNAME', 'INTERNALSIGNEDON',
                  'INTERNALSOURCE', 'INTERNALSOURCEREFNO', 'EXTERNALREFNO', 'EXTERNALVERBALBYKEY',
                  'EXTERNALVERBALBY', 'EXTERNALAPPROVEDBYKEY', 'EXTERNALAPPROVEDBY', 'EXTERNALAPPROVEDON',
                  'EXTERNALSIGNEDBYKEY', 'EXTERNALSIGNEDBY', 'EXTERNALSIGNEDON', 'PERFORMANCEBONDREQUIRED',
                  'PERFORMANCEBONDRECEIVED', 'PERFORMANCEBONDAMOUNT', 'PERFORMANCESURETYCOMPANYKEY',
                  'PERFORMANCESURETYCOMPANY', 'PERFORMANCESURETYCOMPANYNAME', 'PAYMENTBONDREQUIRED',
                  'PAYMENTBONDRECEIVED', 'PAYMENTBONDAMOUNT', 'PAYMENTSURETYCOMPANYKEY', 'PAYMENTSURETYCOMPANY',
                  'PAYMENTSURETYCOMPANYNAME', 'MEGAENTITYKEY', 'MEGAENTITYID', 'MEGAENTITYNAME']