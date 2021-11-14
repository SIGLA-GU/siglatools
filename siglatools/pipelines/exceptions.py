class PrefectFlowFailure(Exception):
    def __init__(self, flow_name: str, **kwargs):
        super().__init__(**kwargs)
        self.flow_name = flow_name

    def __str__(self):
        return f"{self.flow_name} failed."