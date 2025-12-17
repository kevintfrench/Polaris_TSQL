select cir.itemrecordid
from CircItemRecords cir
inner join collections col
  on col.collectionid = cir.AssignedCollectionID
where col.name like 'video games'
  and cir.AssignedBranchID = 17
  and cir.LoanableOutsideSystem = 0