"""Casos pequeños con resultados calculados a mano. Usar una BD de pruebas vacía."""

import importlib.util
import os
import unittest
from decimal import Decimal
from pathlib import Path

import psycopg2

HERE = Path(__file__).parent


def module(name):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


loader = module("load_star")
queries = module("check_queries")


class StarTest(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
        self.addCleanup(self.conn.close)
        self.addCleanup(self.conn.rollback)
        self.cur = self.conn.cursor()
        # Sin DROP: falla si la BD elegida ya contiene estos esquemas.
        self.cur.execute((HERE / "sql/oltp_schema.sql").read_text(encoding="utf-8"))
        self.cur.execute("""
            INSERT INTO oltp.customers VALUES
              ('C1','Ana','ana@example.test','Cali','mass',2000000,CURRENT_DATE-365),
              ('C2','Luis','luis@example.test','Bogotá','premium',5000000,CURRENT_DATE-365);
            INSERT INTO oltp.merchants VALUES ('M1','Tienda','retail','Cali');
            INSERT INTO oltp.payment_methods(method_name) VALUES
              ('credit_card'),('debit_card'),('nequi'),('daviplata'),('pse');
            INSERT INTO oltp.transactions VALUES
              ('T1','C1','M1',1,100,'approved',false,CURRENT_DATE-30),
              ('T2','C1','M1',3,200,'approved',true,CURRENT_DATE-7),
              ('T3','C2','M1',5,300,'declined',true,CURRENT_DATE-1),
              ('T4','C2','M1',2,400,'approved',false,CURRENT_DATE-31),
              ('T5','C2','M1',4,600,'approved',false,CURRENT_DATE-1+INTERVAL '13 hours');
            INSERT INTO oltp.fraud_events VALUES
              ('E1','T2','odd_hours',0.8000,CURRENT_DATE-7),
              ('E2','T2','amount_spike',0.9500,CURRENT_DATE-7),
              ('E3','T3','amount_spike',0.9000,CURRENT_DATE-1);
        """)
        self.cur.execute((HERE / "sql/star_schema.sql").read_text(encoding="utf-8"))
        for _, sql in loader.STEPS:
            self.cur.execute(sql)

    def rows(self, sql):
        self.cur.execute(sql)
        return self.cur.fetchall()

    def test_grain_measures_and_optional_fraud(self):
        self.assertEqual(
            self.rows(
                "SELECT COUNT(*), SUM(amount_cop), COUNT(*) FILTER (WHERE is_fraud) "
                "FROM dw.fact_transactions"
            ),
            [(5, Decimal(1600), 2)],
        )
        self.assertEqual(
            self.rows(
                "SELECT transaction_id, fraud_risk_score FROM dw.fact_transactions "
                "ORDER BY transaction_id"
            ),
            [
                ("T1", None),
                ("T2", Decimal("0.9500")),
                ("T3", Decimal("0.9000")),
                ("T4", None),
                ("T5", None),
            ],
        )
        loader.verify(self.cur)

    def test_queries_exact_results_and_date_boundaries(self):
        olap = queries.read_queries(HERE / "sql/olap_queries.sql")
        oltp = queries.read_queries(HERE / "sql/oltp_problems.sql")
        for a, b in zip(oltp, olap):
            self.assertEqual(sorted(self.rows(a)), sorted(self.rows(b)))
        self.assertEqual(self.rows(olap[0]), [("retail", 3, Decimal(900), Decimal(300))])
        self.assertEqual(
            self.rows(olap[1]),
            [("mass", 2, 1, Decimal("50.00")), ("premium", 3, 1, Decimal("33.33"))],
        )
        expected_days = self.rows(
            "SELECT EXTRACT(DOW FROM CURRENT_DATE-7), EXTRACT(DOW FROM CURRENT_DATE-1)"
        )[0]
        self.assertEqual(
            sorted(self.rows(olap[2])),
            sorted(
                [(expected_days[0], 0, 1, Decimal(200)), (expected_days[1], 13, 1, Decimal(600))]
            ),
        )

    def test_scd2_boundary_and_historical_segment(self):
        # C1 cambia mass -> premium exactamente en T2. T1 debe seguir en mass.
        self.cur.execute("""
            UPDATE dw.dim_customer SET valid_to=CURRENT_DATE-7, is_current=false
            WHERE customer_id='C1';
            INSERT INTO dw.dim_customer(customer_id,city,segment,valid_from,valid_to,is_current)
            VALUES ('C1','Cali','premium',CURRENT_DATE-7,NULL,true);
            DELETE FROM dw.fact_transactions;
        """)
        self.cur.execute(loader.SQL_FACT)
        self.assertEqual(
            self.rows("""SELECT f.transaction_id,c.segment FROM dw.fact_transactions f
            JOIN dw.dim_customer c USING(customer_key) WHERE c.customer_id='C1' ORDER BY 1"""),
            [("T1", "mass"), ("T2", "premium")],
        )
        q2 = queries.read_queries(HERE / "sql/olap_queries.sql")[1]
        self.assertEqual(
            self.rows(q2), [("premium", 4, 2, Decimal("50.00")), ("mass", 1, 0, Decimal("0.00"))]
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
