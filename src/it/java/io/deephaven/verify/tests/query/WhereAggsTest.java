package io.deephaven.verify.tests.query;

import static org.junit.Assert.*;
import org.junit.*;
import io.deephaven.verify.api.Verify;

@Ignore
public class WhereAggsTest {
	final Verify api = Verify.create(this);
	
	@Before
	public void setup() {
//		var query = 
//		"""
//		from deephaven import read_csv
//		from deephaven import agg
//
//		seattleWeather = read_csv("/data/examples/GSOD/csv/seattle.csv")
//		aggs = [
//		    agg.avg("AvgTemp = Temp"),
//		    agg.min_("LoTemp = Temp"),
//		    agg.max_("HiTemp = Temp")
//		]
//		
//		formulas = ["Year = yearNy(ObservationDate)", "Temp = TemperatureF"]
//		""";
//		api.query(query).queryFlight();
	}

	@Test
	public void oneWhere3Aggs() {
		var query = 
		"""
		from deephaven import read_csv
		from deephaven import agg

		seattleWeather = read_csv("/data/examples/GSOD/csv/seattle.csv")
		aggs = [
		    agg.avg("AvgTemp = Temp"),
		    agg.min_("LoTemp = Temp"),
		    agg.max_("HiTemp = Temp")
		]
		
		formulas = ["Year = yearNy(ObservationDate)", "Temp = TemperatureF"]
		hiLoByYear = seattleWeather.view(formulas=formulas).where("Year >= 2000").agg_by(aggs, "Year")
		""";
		
		api.query(query).fetchAfter("hiLoByYear", table->{
			assertEquals("Wrong row count", 0, table.getRowCount());
		}).execute();
//		api.waitForResults();
	}

	@After
	public void teardown() {
		api.close();
	}

}
