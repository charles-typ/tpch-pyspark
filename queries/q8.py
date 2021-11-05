query = """
SELECT  O_YEAR,
        sum(CASE WHEN NATION='BRAZIL' THEN VOLUME ELSE 0 END) / sum(VOLUME) AS MKT_SHARE
FROM    (
                SELECT  EXTRACT(YEAR FROM O_ORDERDATE) AS O_YEAR,
                        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
                        n2.N_NAME AS NATION
                FROM    part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                WHERE   P_PARTKEY = L_PARTKEY
                        AND S_SUPPKEY = L_SUPPKEY
                        AND L_ORDERKEY = O_ORDERKEY
                        AND O_CUSTKEY = C_CUSTKEY
                        AND C_NATIONKEY = n1.N_NATIONKEY
                        AND n1.N_REGIONKEY = R_REGIONKEY
                        AND R_NAME = 'AMERICA'
                        AND S_NATIONKEY = n2.N_NATIONKEY
                        AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
                        AND P_TYPE = 'ECONOMY ANODIZED STEEL'
        ) AS ALL_NATIONS
GROUP BY    O_YEAR
ORDER BY    O_YEAR
"""
