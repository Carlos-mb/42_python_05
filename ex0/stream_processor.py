from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """Abstract base class for processing different types of data.

    This class defines the interface for data processors that can validate,
    process, and format output for various data types.
    """

    @abstractmethod
    def process(self, data: Any) -> str:
        """Process the input data and return a string result.

        Args:
            data: The data to process (type depends on implementation).

        Returns:
            A string containing the processed result.
        """
        ...

    def validate(self, data: Any) -> bool:
        """Validate if the input data is of the correct type.

        Args:
            data: The data to validate.

        Returns:
            True if data is valid, False otherwise.
        """
        ...

    def format_output(self, result: str) -> str:
        """Format the output result.

        Args:
            result: The result string to format.

        Returns:
            The formatted result string.
        """
        return result


class NumericProcessor(DataProcessor):
    """Processor for numeric data (lists of numbers).

    Validates and processes lists of numeric values, calculating statistics
    like sum and average.
    """

    def validate(self, data: Any) -> bool:
        """Validate that the data is a list of numeric values.

        Args:
            data: The data to validate.

        Returns:
            True if data is a list of numbers, False otherwise.
        """
        try:
            for num in data:
                num.is_integer
        except (AttributeError, TypeError):
            print("Not list of numbers")
            return False
        print("Validation: Numeric data verified")
        return True

    def process(self, data: Any) -> str:
        """Process numeric data and calculate statistics.

        Args:
            data: A list of numeric values.

        Returns:
            A string with count, sum, and average of the values.
        """
        return (
            f"Processed {len(data)} numeric values "
            f", sum={sum(data)}, avg={sum(data) / len(data)}"
        )


class TextProcessor(DataProcessor):
    """Processor for text data (strings).

    Validates and processes text strings, analyzing character and word counts.
    """

    def validate(self, data: Any) -> bool:
        """Validate that the data is a string.

        Args:
            data: The data to validate.

        Returns:
            True if data is a string, False otherwise.
        """
        try:
            data.capitalize()
        except (AttributeError, TypeError):
            print("Not a string")
            return False
        print("Validation: Text data verified")
        return True

    def process(self, data: Any) -> str:
        """Process text data and analyze its structure.

        Args:
            data: A string to analyze.

        Returns:
            A string with character and word count statistics.
        """
        return f"Processed text: {len(data)} chars, {len(data.split())} words"


class LogProcessor(DataProcessor):
    """Processor for log entries.

    Validates and processes log messages, detecting ERROR and INFO levels
    and formatting them appropriately.
    """

    def validate(self, data: Any) -> bool:
        """Validate that the data is a log entry with ERROR or INFO level.

        Args:
            data: The data to validate.

        Returns:
            True if data is a valid log entry, False otherwise.
        """
        try:
            if data.find("ERROR") > -1 or data.find("INFO") > -1:
                print("Validation: Log entry verified")
                return True
        except (AttributeError, TypeError):
            print("Not a log entry")
            return False
        return False

    def process(self, data: Any) -> str:
        """Process log data and format by severity level.

        Args:
            data: A log entry string.

        Returns:
            A formatted log message with appropriate alert level.
        """
        output: str = ""
        if data.find("ERROR") > -1:
            output = "[ALERT] ERROR level detected: " + data[7:]
        elif data.find("INFO") > -1:
            output = "[INFO] INFO level detected: " + data[6:]
        return output

    def format_output(self, result: str) -> str:
        """Override the format_output method for log-specific formatting.

        Args:
            result: The result string to format.

        Returns:
            The formatted result with log prefix.
        """
        return "Overiden log:" + result


def main() -> None:
    """Main function demonstrating polymorphic data processing.

    Tests different processor implementations with various data types,
    showing validation, processing, and output formatting capabilities.
    """
    print("Initializing Numeric Processor...")
    print("Processing data: [1, 2, 3, 4, 5]")
    lst: list[int] = [1, 2, 3, 4, 5]
    processor = NumericProcessor()
    if processor.validate(data=lst):
        print(processor.process(data=lst))
    # Just to check
    print("\nError check:")
    processor.validate("Hi!")

    print("")
    print("Initializing Text Processor...")
    print('Processing data: "Hello Nexus World"')
    processor = TextProcessor()
    if processor.validate("Hello Nexus World"):
        print(processor.process("Hello Nexus World"))
    # Just to check
    print("\nError check:")
    processor.validate(45)

    print("")
    print("Initializing Log Processor...")
    print('Processing data: "ERROR: Connection timeout"')
    processor = LogProcessor()
    if processor.validate("ERROR: Connection timeout"):
        print(processor.process("ERROR: Connection timeout"))
    # Just to check
    print("\nError check:")
    processor.validate(45)

    print("\n=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    polimorph: list[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
    ]

    print("Result 1: ", polimorph[0].process([6, 0, 0]))
    print("Result 2: ", polimorph[1].process("ABCDEF ABCDE"))
    print("Result 3: ", polimorph[2].process("INFO: System ready"))

    print("\n=== Override Demo. Only Log has been overrided ===")
    print("Result 1: ", polimorph[0].format_output("For numbers"))
    print("Result 2: ", polimorph[1].format_output("For texts"))
    print("Result 3: ", polimorph[2].format_output("For Log"))


if __name__ == "__main__":
    main()
