import aws_cdk as cdk

from clinical_trials.clinical_trials_stack import MskDemoStack


app = cdk.App()
MskDemoStack(app, "msk-demo")

app.synth()