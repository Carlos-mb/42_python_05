from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        """Initialize stream metadata and counters.

        Args:
            stream_id: Unique identifier for the stream.
        """
        self.processed_count: int = 0
        self.stream_id: str = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data and return a summary string.

        Args:
            data_batch: Items to process.
        """
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Return a filtered batch based on optional criteria.

        Args:
            data_batch: Items to filter.
            criteria: Optional filter criteria.
        """
        return data_batch

    def get_stats(self) -> Dict[str, str | int | float]:
        """Return basic stream statistics.

        Args:
            None.
        """
        return {
            "stream_id": self.stream_id,
            "processed_count": self.processed_count
        }


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        """Initialize a sensor stream.

        Args:
            stream_id: Unique identifier for the stream.
        """
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process sensor readings and report the average.

        Args:
            data_batch: Sensor readings to process.
        """
        try:
            average = sum(data_batch) / len(data_batch)
            self.processed_count += len(data_batch)
            return (
                f"[{self.stream_id}] "
                f"{len(data_batch)} readings processed, avg: {average:.2f}"
            )

        except (TypeError, ZeroDivisionError):
            return f"[{self.stream_id}] Invalid sensor data"

    def filter_data(
                    self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter sensor readings by criteria.

        Args:
            data_batch: Sensor readings to filter.
            criteria: Filter criteria for readings.
        """
        try:
            if criteria == "high":
                return [data for data in data_batch if data > 30]
            elif criteria == "standard":
                return [data for data in data_batch if data <= 30]
        except TypeError:
            return data_batch
        return data_batch

    def get_stats(self) -> Dict[str, str | int | float]:
        """Return basic stream statistics with personalized text"""
        return {
            "stream_id": "Sensor: " + self.stream_id,
            "processed_count": self.processed_count
        }


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        """Initialize a transaction stream.

        Args:
            stream_id: Unique identifier for the stream.
        """
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process transactions and report the net flow.

        Args:
            data_batch: Transaction values to process.
        """
        try:
            total = sum(data_batch)
            self.processed_count += len(data_batch)
            return (
                f"[{self.stream_id}] "
                f"{len(data_batch)} operations processed, net flow: {total}"
            )

        except TypeError:
            return f"[{self.stream_id}] Invalid transaction data"

    def filter_data(
                    self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None
                ) -> List[Any]:
        """Filter transactions by criteria.

        Args:
            data_batch: Transaction values to filter.
            criteria: Filter criteria for transactions.
        """

        if criteria is None:
            return data_batch
        try:
            if criteria == "positive":
                return [data for data in data_batch if data >= 0]
            elif criteria == "negative":
                return [data for data in data_batch if data < 0]
        except (TypeError, ValueError):
            return data_batch

        return data_batch

    def get_stats(self) -> Dict[str, str | int | float]:
        """Return basic stream statistics with personalized text"""
        return {
            "stream_id": "Transactions: " + self.stream_id,
            "processed_count": self.processed_count
        }


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        """Initialize an event stream.

        Args:
            stream_id: Unique identifier for the stream.
        """
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process events and report error counts.

        Args:
            data_batch: Event entries to process.
        """
        try:
            total = len(data_batch)
            error_count = sum(
                1 for event in data_batch
                if isinstance(event, str) and "error" in event
            )
            self.processed_count += total
            return (
                f"[{self.stream_id}] "
                f"{total} events processed, {error_count} error(s)"
            )

        except TypeError:
            return f"[{self.stream_id}] Invalid event data"

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        """Filter events by criteria.

        Args:
            data_batch: Event entries to filter.
            criteria: Filter criteria for events.
        """

        if criteria is None:
            return data_batch

        try:
            if criteria == "error":
                return [data for data in data_batch if "error" in data]
            elif criteria == "info":
                return [data for data in data_batch if "error" not in data]
        except (TypeError, ValueError):
            return data_batch

        return data_batch

    def get_stats(self) -> Dict[str, str | int | float]:
        """Return basic stream statistics with personalized text"""
        return {
            "stream_id": "Events: " + self.stream_id,
            "processed_count": self.processed_count
        }


class StreamProcessor:

    def __init__(self, streams: List[DataStream]) -> None:
        """Initialize the processor with streams.

        Args:
            streams: Streams to process.
        """
        self.streams: List[DataStream] = streams

    def process_streams(
                        self,
                        data_map: Dict[str, List[Any]]) -> None:
        """Process all streams using the provided data map.

        Args:
            data_map: Mapping of stream IDs to data batches.
        """

        for stream in self.streams:
            batch: list[Any] = data_map.get(stream.stream_id, [])
            try:
                result: str = stream.process_batch(batch)
                print(result)
            except Exception as e:
                print(f"Processing error in {stream.stream_id}: {e}")

    def filter_streams(
            self,
            data_map: Dict[str, List[Any]],
            criteria_map: Dict[str, str]) -> None:
        """Filter all streams using the provided criteria map.

        Args:
            data_map: Mapping of stream IDs to data batches.
            criteria_map: Mapping of stream IDs to filter criteria.
        """

        for stream in self.streams:
            batch = data_map.get(stream.stream_id, [])
            criteria = criteria_map.get(stream.stream_id)
            try:
                filtered = stream.filter_data(batch, criteria)
                print(
                    f"{stream.stream_id}: "
                    f"{len(filtered)} filtered item(s)"
                )
            except Exception as e:
                print(f"Filtering error in {stream.stream_id}: {e}")


def main() -> None:

    data_streams: List[DataStream] = [
        SensorStream("SENSOR_001"),
        TransactionStream("TRANS_001"),
        EventStream("EVENT_001")
    ]

    data_lists: Dict[str, List[Any]] = {
        "SENSOR_001": [22.5, 50, 21.8],
        "TRANS_001": [100.00, 150.00, -75.00],
        "EVENT_001": ["login", "error", "logout"]
    }

    processor = StreamProcessor(data_streams)

    print("=== Batch Processing ===")
    processor.process_streams(data_lists)

    print("\n=== Filtering ===")

    filter_criteria: Dict[str, str] = {
        "SENSOR_001": "high",
        "TRANS_001": "negative",
        "EVENT_001": "error"
    }

    processor.filter_streams(data_lists, filter_criteria)

    print("\n=== Stream Statistics ===")
    for stream in data_streams:
        print(stream.get_stats())


if __name__ == "__main__":
    main()
