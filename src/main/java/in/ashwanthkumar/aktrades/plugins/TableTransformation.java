package in.ashwanthkumar.aktrades.plugins;

import tech.tablesaw.api.Table;

public interface TableTransformation {
    /**
     * Transform the parsed Table
     *
     * @param input Table that is parsed from the CSV
     * @return Table that with the all the required columns parsed in the right format.
     */
    Table transform(Table input);
}
