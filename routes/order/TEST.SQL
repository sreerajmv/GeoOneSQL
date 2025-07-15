                   SELECT 

                        SUM(CAST(ISNULL(CASE 
                            WHEN D.UomCode = 'SQM' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                            WHEN D.UomCode = 'KGS' THEN TRY_CAST(A.Qty AS NUMERIC(10,2))/1000
                            WHEN D.UomCode = 'MTR' THEN TRY_CAST((A.Qty*TRY_CAST(C.Width AS NUMERIC(10,2))) * TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                            WHEN D.UomCode = 'NOS' THEN TRY_CAST(A.Qty*TRY_CAST(C.altuntcom1 AS NUMERIC(10,2)) AS DECIMAL(18,2))/1000
                            ELSE 0 
                        END, 0) AS NUMERIC(10,2))) AS total_tonnage
    
                    FROM 
                        TBL_SalesOrderProductDetails A
                        INNER JOIN TBL_SalesOrderDetails B ON A.SOID = B.SlNo
                        INNER JOIN ItemMaster_M_Tbl C ON C.ItemCode = A.ProductCode
                        INNER JOIN Uom_Master_M_Tbl D ON D.UomId = C.UOM
                        INNER JOIN CustomerMaster_M_Tbl E ON E.CardCode = B.CustCode
                        INNER JOIN LocationMaster_M_Tbl F ON F.Code = B.LocationID
                        INNER JOIN TBL_Users U ON U.UserID=A.MakerID
                        INNER JOIN SalesEmployeeMaster_M_Tbl SE ON B.SalesPerson=SE.SalesEmployeeCode
                        INNER JOIN Employee_Master_M_Tbl EM ON EM.SapEmployeeId=SE.EmployeeId