

--////////////////////////////////////////////////////////////
--Original Lanaguage: Google BigQuery (SQL Platform Equivalent)

-- Business Usecase: 
-- Need to Identify Cases of ACH Kiting for INvestigator Review
-- ACH Kiting is a fraud abuse scheme in which a frauster take advantage of a financial institutions early availbility of funds, knowing they did not have the sufficient funds or access to the funds in the original deposit.

-- For example, Fraudster A has $10 in their Wells Fargo account. Fraudster A goes to their FinTech Bank Account and initiates a deposit for $30 to their Fintech Bank Account.

-- By NACHA requirements, Wells Fargo has up to 3 banking business days to file a return on the transaction request and identify that there are insufficient funds in its account to cover the $30 deposit request.
-- This would be done by filing an R01 return, noted for insufficient funds.

-- However, FinTech Bank Account wants to be very competitive in this space and while most banks will hold the funds for the allotted 3 days, FinTech Bank will release teh funds only 2 days after settlement.
-- Frauster A knowns this. And so when the $30 are made available to them on day 2, before Wells Fargo has the chance to submit the R01, Fraudster A withdraws the $30 out of their account through whatever means they see fit.
-- On day 3, the R01 is recieved from Wells Fargo and FinTech Bank is regulatorily required to return the $30 back to Wells Fargo. However, Fraudster A's account balance is currently $0 and thus FinTech bank must pull from the fraud reserve to cover this loss.

-- A seasoned and well equipped fraduster will try and do this multiple times, depending on the controls set at the bank side to limit fraud loss exposure.
-- Secondarily, a fraudster who knows they may only have one chance will kite a much larger amount than $30.
-- The bank does have some legal options to pursue agaisnt the fraudster for that loss, but the legal costs often outeigh the loss incurred and thus this is a very lucrative tactic for our fraudster.

--The following query identifies cases in which ACH Kiting occurs for an investigation to be performed on the member, as well as to track the amount of fraud loss or attemtped fraud loss via this specific scheme.

-- NOTE: All deatils including table name, columns names, and identifying variables have been obfuscated from the original usecase
--////////////////////////////////////////////////////////////


select 
  tr.customerId
, tr.accountNumber
, tr.transactionMasterID
, tr.transactionID
, tr.TransactionType
, (tr.transactionAmount/100) as deposit_transactionAmount

, datetime(tr.transactionInitiated, "America/New_York") as deposit_transactionCreatedDate
, datetime(tr.transactionSettled, "America/New_York") as deposit_transactionSettledDate
, datetime(tr.transactionAvailable, "America/New_York") as deposit_transactionAvailableDate

, returns.transactionReturnedDate as deposit_returnedDate
, returns.returnCode as deposit_returnCode

--Calcualte the withdrawal activity between when the funds were made available ti the customer and when a return was recieved
, min(withdrawl.transactionCreatedDate) as withdrawl_transactionCreatedDate_start
, max(withdrawl.transactionCreatedDate) as withdrawl_transactionCreatedDate_end
, round(sum(withdrawl.transactionAmount),2) as withdrawal_transactionAmount_sum
, count(distinct withdrawl.transactionmasterId) as withdrawal_transactionVolume

FROM `vendor_schema.transactions` tr

--////////////////////////////////
-- Identify Returned Transactions
--////////////////////////////////

left join 
(

select transactionMasterID
, datetime(returnDate, "America/New_York") as transactionReturnedDate
, returnCode
FROM `vendor_schema.returns`
where upper(returnCode) IN('R01', 'R02', 'R03', 'R16', 'R21') --Limit only to target returns for ACH Kiting assessment (Note: This is a subset of returns used for ACH kiting)

) returns
on returns.transactionMasterID = tr.transactionMasterID

--////////////////////////////////
-- Identify Withdrawal Transactions
--////////////////////////////////

 left join 
 (
select 
  tr.customerId
, tr.accountNumber
, tr.transactionMasterID
, tr.transactionID
, tr.TransactionType
, (tr.transactionAmount/100) as transactionAmount

, datetime(tr.transactionInitiated, "America/New_York") as transactionCreatedDate
 FROM `vendor_schema.transactions` tr
 
 where enf.data.transactionType IN(

    SELECT transactionType as transactionType
    FROM `vendor_schema.transactionTypes`
    where accountImpact = 'Debit' --Only include any transaction in which money movement is moving out of the account
    and upper(trnasctionType) not like '%RTN%' --Remove any money moving out of the account due to return activity
    group by 1
 )

 ) withdrawl
 on withdrawl.customerId = tr.customerId


where
--Limit to ACH Deposits initiated that are subject to ACH Kiting 
tr.TransactionType = 'ACH Deposit'

--Identify instances where a return is recieved AFTER the funds have been made available 
and returns.transactionReturnedDate >= datetime(tr.transactionAvailable, "America/New_York") 

--Limit to only transactions which recieved a return in our relevant population, defined above
and return_code.returnCode is null

--Finally, we are only interested in the cases which meet the above 3 criteria IF there was a withdrawal initiated inbetween the funds availabilty and the return
and withdrawl.transactionCreatedDate between datetime(tr.transactionAvailable, "America/New_York")  and returns.transactionReturnedDate

group by 1,2,3,4,5,6,7,8,9,10,11

--Lastly, we only wish to see cases where there was more withdrawan than what was originally returned. These cases will help identify the accounts most at risk for investgator due dilligence.
having round(sum(withdrawl.transactionAmount),2) >= (tr.transactionAmount/100)
