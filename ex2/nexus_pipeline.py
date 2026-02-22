from abc import ABC, abstractmethod
from typing import Protocol, Any


class ProcessingStage(Protocol):
    """Does not create a real class, only defines an interface.
    Any class with method 'process' will be valid as ProcessingStage
    """

    def process(self, data: Any) -> Any:
        """Do not use 'pass' because it is not real.
        pass is also valid, becuse it creates an empty method.
        '...' ex more explicit telling that this define only the signature,
        does not create a class or method"""
        ...


class InputStage:
    def process(self, data: Any) -> Any:
        print("Stage Input: Validating and parsing data")
        return data


class TransformStage:
    def process(self, data: Any) -> Any:
        print("Stage Transform: Enriching data")
        if data == "FAIL":
            raise ValueError("Invalid data format")
        return data


class OutputStage:
    def process(self, data: Any) -> Any:
        print("Stage Output: Formatting result")
        return data


class ProcessingPipeline(ABC):

    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id: str = pipeline_id
        self.stages: list[ProcessingStage] = []
        self.processed_count: int = 0

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def run_stages(self, data: Any) -> Any:
        current_data: Any = data

        for stage in self.stages:
            try:
                current_data = stage.process(current_data)
            except (TypeError, ValueError) as e:
                print(f"Error detected in stage: {e}")
                print("Recovery initiated: Switching to safe mode")

                # Fallback strategy
                current_data = f"[RECOVERED]{current_data}"

                print("Recovery successful. Continuing pipeline.")

        return current_data

    @abstractmethod
    def process(self, data: Any) -> Any:
        """The modern way is (str | Any) instead of Union[str, Any]"""
        ...


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        print("Adapter: Processing JSON data through pipeline...")
        print(f"Receiving: {data}\n")
        result = self.run_stages(data)
        self.processed_count += 1
        return f"Processed JSON data: {result}"


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        print("Adapter: Processing CSV data through same pipeline...")
        print(f"Receiving: {data}\n")
        result = self.run_stages(data)
        self.processed_count += 1
        return f"Processed CSV data: {result}"


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> Any:
        print("Adapter: Processing Stream data through same pipeline...")
        print(f"Receiving: {data}\n")
        result = self.run_stages(data)
        self.processed_count += 1
        return f"Processed STREAM data: {result}"


class NexusManager():
    def __init__(self) -> None:
        self.pipelines: list[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def execute_all(self, data: Any) -> None:
        """“Execute all registered pipelines independently.”"""
        for pipeline in self.pipelines:
            try:
                result: Any = pipeline.process(data)
                print(f"Nexus Manager result: {result}\n")
            except (TypeError, ValueError) as e:
                print(f"Error procesing data {e}")

    def execute_chain(self, data: Any) -> Any:
        """This is a pipeline.
        The output of one stage is the input of the next"""

        current_data = data
        i: int = 1
        for pipeline in self.pipelines:
            print(f"\nChain pass {i}: ")
            current_data = pipeline.process(current_data)
            i = i + 1
        return current_data


def main():
    """
    main()
    ↓
    NexusManager
    ↓
    ProcessingPipeline
    ↓
    Stages
    """

    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")
    print("Initializing Nexus Manager...")

    manager = NexusManager()

    print("Creating json pipeline with 3 stages")
    json_pipeline = JSONAdapter("JSON_01")
    json_pipeline.add_stage(InputStage())
    json_pipeline.add_stage(TransformStage())
    json_pipeline.add_stage(OutputStage())

    print("Creating csv pipeline with 3 stages")
    csv_pipeline = CSVAdapter("CSV_01")
    csv_pipeline.add_stage(InputStage())
    csv_pipeline.add_stage(TransformStage())
    csv_pipeline.add_stage(OutputStage())

    print("Creating stream pipeline with 3 stages")
    stream_pipeline = StreamAdapter("STREAM_01")
    stream_pipeline.add_stage(InputStage())
    stream_pipeline.add_stage(TransformStage())
    stream_pipeline.add_stage(OutputStage())

    print("Adding 3 pipelines to Pipeline Manager")
    manager.add_pipeline(json_pipeline)
    manager.add_pipeline(csv_pipeline)
    manager.add_pipeline(stream_pipeline)

    print("Using dummy data")
    input_data: str = "TEST DATA"

    print("\n=== Multi-Format Data Processing ===")
    print("Calling all pipelines. Each pipeline has 3 stages.\n")
    manager.execute_all(input_data)

    print("\n=== Statistics ===")
    for pipeline in manager.pipelines:
        print(f"Pipeline {pipeline.pipeline_id}: {pipeline.processed_count}")

    print("\n=== Pipeline Chaining Demo ===")
    print("Calling all pipelines chaining output -> input")

    final = manager.execute_chain(input_data)
    print("Chain result:", final)

    print("\n=== Statistics ===")
    for pipeline in manager.pipelines:
        print(f"Pipeline {pipeline.pipeline_id}: {pipeline.processed_count}")

    print("\n=== Error Recovery Test ===")
    print("Simulating pipeline failure...\n")

    manager.execute_all("FAIL")


if __name__ == "__main__":
    main()
