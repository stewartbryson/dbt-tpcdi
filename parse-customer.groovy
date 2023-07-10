#!/opt/homebrew/bin/groovy
import groovy.xml.*

def xmlFile = new File('/Users/stewartbryson/dev/tpc-di-output/Batch1/CustomerMgmt.xml')

def csvFile = new File('/Users/stewartbryson/dev/tpc-di-output/Batch1/CustomerMgmt.csv')

// write the header file
csvFile.write 'ACTION_TYPE|ACTION_TS|C_ID|C_TAX_ID|C_GNDR|C_TIER|C_DOB|C_L_NAME|C_F_NAME|C_M_NAME|C_ADLINE1|C_ADLINE2|C_ZIPCODE|C_CITY|C_STATE_PROV|C_CTRY|C_PRIM_EMAIL|C_ALT_EMAIL|C_PHONE_1|C_PHONE_2|C_PHONE_3|C_LCL_TAX_ID|C_NAT_TAX_ID|CA_ID|CA_TAX_ST|CA_B_ID|CA_C_ID|CA_NAME\n'

def actions = new XmlSlurper().parseText(xmlFile.text)

actions.Action.each { action ->

    //write the columns
    csvFile.append action.@ActionType.toString() + '|'
    csvFile.append action.@ActionTS.toString() + '|'
    csvFile.append action.Customer.@C_ID.toString() + '|'
    csvFile.append action.Customer.@C_TAX_ID.toString() + '|'
    csvFile.append action.Customer.@C_GNDR.toString() + '|'
    csvFile.append action.Customer.@C_TIER.toString() + '|'
    csvFile.append action.Customer.@C_DOB.toString() + '|'
    csvFile.append action.Customer.Name.C_L_NAME.toString() + '|'
    csvFile.append action.Customer.Name.C_F_NAME.toString() + '|'
    csvFile.append action.Customer.Name.C_M_NAME.toString() + '|'
    csvFile.append action.Customer.Address.C_ADLINE1.toString() + '|'
    csvFile.append action.Customer.Address.C_ADLINE2.toString() + '|'
    csvFile.append action.Customer.Address.C_ZIPCODE.toString() + '|'
    csvFile.append action.Customer.Address.C_CITY.toString() + '|'
    csvFile.append action.Customer.Address.C_STATE_PROV.toString() + '|'
    csvFile.append action.Customer.Address.C_CTRY.toString() + '|'
    csvFile.append action.Customer.ContactInfo.C_PRIM_EMAIL.toString() + '|'
    csvFile.append action.Customer.ContactInfo.C_ALT_EMAIL.toString() + '|'
    csvFile.append action.Customer.ContactInfo.C_PHONE_1.iterator().join(".").toString().replace('-','.') + '|'
    csvFile.append action.Customer.ContactInfo.C_PHONE_2.iterator().join(".").toString().replace('-','.') + '|'
    csvFile.append action.Customer.ContactInfo.C_PHONE_3.iterator().join(".").toString().replace('-','.') + '|'
    csvFile.append action.Customer.TaxInfo.C_LCL_TX_ID.toString() + '|'
    csvFile.append action.Customer.TaxInfo.C_NAT_TX_ID.toString() + '|'
    csvFile.append action.Customer.Account.@CA_ID.toString() + '|'
    csvFile.append action.Customer.Account.@CA_TAX_ST.toString() + '|'
    csvFile.append action.Customer.Account.CA_B_ID.toString() + '|'
    csvFile.append action.Customer.Account.CA_C_ID.toString() + '|'
    csvFile.append action.Customer.Account.CA_NAME.toString()

    // close the record
    csvFile.append '\n'

}