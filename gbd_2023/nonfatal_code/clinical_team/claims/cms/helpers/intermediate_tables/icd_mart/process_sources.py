from dataclasses import dataclass


@dataclass
class SourceProcessing:
    name: str
    requires_sample: bool


Max = SourceProcessing(name="max", requires_sample=False)

Mdcr = SourceProcessing(name="mdcr", requires_sample=True)


def get_source_processing(name: str) -> SourceProcessing:
    if name == "mdcr":
        return Mdcr
    elif name == "max":
        return Max
    else:
        raise RuntimeError(f"Source {name} was not recognized")
