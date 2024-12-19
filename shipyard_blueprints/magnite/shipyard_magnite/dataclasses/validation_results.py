from dataclasses import dataclass, asdict
from typing import Optional, Dict


@dataclass
class ValidationResult:
    status: str
    message: str
    details: Dict[str, Optional[str]]

    @staticmethod
    def report(results: list["ValidationResult"]) -> str:
        """
        Generate a formatted report of multiple validation results. The report is formatted as a table.

        Args:
            results (List[ValidationResult]): A list of validation results to display.

        Returns:
            str: A table summarizing all validation results.

        Example:

        Status   | Message                          | id        | budget_value | budget_period | budget_pacing | budget_metric   | vast_caching_adjustment
        ---------|----------------------------------|-----------|--------------|---------------|---------------|-----------------|--------------------------
        OK       | Valid budget item.               | 1         | 100.0        | daily         | smooth        | clicks          | None
        ERROR    | Invalid budget metric: invalid   | 2         | -50.0        | weekly        | asap          | invalid         | None
        OK       | Valid budget item.               | 3         | 200.0        | monthly       | smooth        | impressions     | None
        """
        static_headers = ["Status", "Message"]
        all_dynamic_headers = set()

        for result in results:
            all_dynamic_headers.update(result.details.keys())

        dynamic_headers = sorted(all_dynamic_headers)
        headers = static_headers + dynamic_headers

        rows = []
        for result in results:
            static_values = [result.status, result.message]
            dynamic_values = [
                result.details.get(header, "None") for header in dynamic_headers
            ]
            row = static_values + dynamic_values
            rows.append(row)

        column_widths = [
            max(len(str(row[i])) for row in rows + [headers])
            for i in range(len(headers))
        ]
        separator = " | ".join("-" * width for width in column_widths)
        header_row = " | ".join(
            f"{header:<{width}}" for header, width in zip(headers, column_widths)
        )

        formatted_rows = [
            " | ".join(
                f"{str(value):<{width}}" for value, width in zip(row, column_widths)
            )
            for row in rows
        ]

        table = f"{header_row}\n{separator}\n" + "\n".join(formatted_rows)
        return table
