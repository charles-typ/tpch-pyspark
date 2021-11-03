query = """
SELECT  S_ACCTBAL
        S_NAME,
        N_NAME,
        P_PARTKEY,
        P_MFGR,
        S_ADDRESS,
        S_PHONE,
        S_COMMENT
FROM    part,
        supplier,
        partsupp,
        nation,
        region
WHERE   P_PARTKEY = PS_PARTKEY
AND     S_SUPPKEY = PS_SUPPKEY
AND     P_SIZE = : 1
AND     P_TYPE LIKE '%:2'
AND     S_NATIONKEY = N_NATIONKEY
AND     N_REGIONKEY = R_REGIONKEY
AND     R_NAME = ':3'
AND     PS_SUPPLYCOST = (
            SELECT  MIN(PS_SUPPLYCOST)
            FROM    PARTSUPP,
                    SUPPLIER,
                    NATION,
                    REGION
            WHERE   P_PARTKEY=PS_PARTKEY
            AND     S_SUPPKEY=PS_SUPPKEY
            AND     S_NATIONKEY=N_NATIONKEY
            AND     N_REGIONKEY=R_REGIONKEY
            AND     R_NAME=':3'
        )
ORDER BY    S_ACCTBAL DESC,
            N_NAME,
            S_NAME,
            P_PARTKEY 
"""
