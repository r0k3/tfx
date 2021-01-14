# Apache Beam and TFX

[Apache Beam](https://beam.apache.org/) provides a framework for running batch
and streaming data processing jobs that run on a variety of execution engines.
Several of the TFX libraries use Beam for running tasks, which enables a high
degree of scalability across compute clusters.  Beam includes support for a
variety of execution engines or "runners", including a direct runner which runs
on a single compute node and is very useful for development, testing, or small
deployments.  Beam provides an abstraction layer which enables TFX to run on any
supported runner without code modifications.  TFX uses the Beam Python API, so
it is limited to the runners that are supported by the Python API.

## Deployment and Scalability

As workload requirements increase Beam can scale to very large deployments
across large compute clusters. This is limited only by the scalability of the
underlying runner.  Runners in large deployments will typically be deployed to a
container orchestration system such as Kubernetes or Apache Mesos for automating
application deployment, scaling, and management.

See the [Apache Beam](https://beam.apache.org/) documentation for more
information on Apache Beam.

For Google Cloud users, [Dataflow](https://cloud.google.com/dataflow) is the
recommended runner, which provides built-in horizontal scalability, security,
and monitoring.

## Custom Python Code and Dependencies

One notable complexity of using TFX over Beam is handling custom code and/or
dependencies needed from additional user modules. Several possibilities of how
this could happen including but not limited to:

*   preprocessing_fn need to refer to user's own python module
*   custom extractor for Evaluator component
*   custom modules which sub-classed TFX component

TFX relies on Beam's support on
[Managing Python Pipeline Dependencies](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/)
to handle Python dependencies. There are two possible ways for user to do this:

### Providing Python Code and Dependencies as Source Package

This is recommended for users who 1) are familiar with python packaging and 2)
only uses Python source code (i.e, no C module or shared libraries). Please
follow one of the paths in
[Managing Python Pipeline Dependencies](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/)
to provide this to one of the following beam_pipeline_args:

*   --setup_file
*   --extra_package
*   --requirements_file

**Notice**: In any of above cases, please make sure that same version of `tfx`
is listed out as a dependency.

### [Dataflow only] Using a Container Image as Worker

TFX 0.26.0 and above has experimental support for using
[custom container image](https://beam.apache.org/documentation/runtime/environments/#customizing-container-images)
as Dataflow workers.

In order to use this, you have to:

*   Build a Docker image which has both `tfx` and users' custom code and
    dependencies pre-installed. For users who uses `tfx>=0.26`, the easiest way
    to do this is extending the corresponding version of official
    `tensorflow/tfx` image:

```Dockerfile
# You can use a build-arg to dynamically pass in
# version of TFX being used to your Dockerfile.

ARG TFX_VERSION
FROM tensorflow/tfx:${TFX_VERSION}
# COPY your code and dependencies in
```

*   Push the image built to a container image registry which is accessible by
    the project used by Dataflow.
    *   Google Cloud users can consider use
        [Cloud Build](https://cloud.google.com/cloud-build/docs/quickstart-build)
        which nicely automate above steps.
*   Provide following `beam_pipeline_args`:

```python
beam_pipeline_args.extend([
    '--runner=dataflow',
    '--project={project-id}',
    '--worker_harness_container_image={image-ref}',
    '--experiments=use_runner_v2',
    '--no_pipeline_type_check',
])
```
