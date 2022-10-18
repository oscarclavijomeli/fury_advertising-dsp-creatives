
<p align="center">
<h1 align="center"> Fury Data App - repository boilerplate </h1>
</p>
<p align="center">
   <img src="https://github.com/mercadolibre/fury_ml-platform/blob/feature/initial-scaffolding/docs/guide/assets/fda.png" width="75" alt="FDA">
</p>

## Quickstart

If don't have `poetry` already, [install it](https://python-poetry.org/docs/#osx--linux--bashonwindows-install-instructions)

1. `poetry install` will install all of your app's dependencies.
2. ????
3. PROFIT!!!

<img src="https://i.imgflip.com/5myrdu.jpg" width="400" title="made at imgflip.com"/>

Now you are ready to unleash the full potential of MeLi's machine learning and data science platform.

### ¿Are you new to FDA 2?

Here are some useful resources to get you started:

* **Talk**: *Un recorrido guiado por FDA 2.0 [in Spanish]*
    * [video](https://drive.google.com/file/d/10A3oZbbLuPuEkqkFCSrCEHBGPuSISmzB/view?usp=sharing)
    * [slides](https://docs.google.com/presentation/d/1NS5Ycx-J4bvCxCZ3zv-KFauXjU_AD1tFGu4zPGVz6nM/edit?usp=sharing)

* The [main **documentation**](http://furydocs.io/molida/guide) will tell you how to
   * `prepare`, `run` and `schedule` your ETLs, trainings and other tasks
   * `build` and `deploy` your MPIs
   * etc

* Wanna know the main **differences with FDA 1**? [This tutorial](https://furydocs.io/molida/guide/#/differences) will help

* For **Community Support**, join the [#community-fda2](https://meli.slack.com/archives/C0232MU9VU1) Slack channel.

* Operative **support**: [Fury Support Tool](https://web.furycloud.io/help) (Category: _Data Apps_)

* For general Q&A and troubleshooting, check [Answerhub](https://mercadolibre.cloud.answerhub.com/topics/fda.html)

* Join our [Workplace group](https://meli.workplace.com/groups/fury.data.apps/) for general communications

* Use [Fury Issues](https://github.com/mercadolibre/fury/issues/new?assignees=&labels=FDA%2C+enhancement%2C+proposed&template=feature-request.md&title=) for bug reports y feature requests


## Initial repository structure

* This is a _minimalistic_ initial repository.
* It aims to reduce the initial technical debt
    * Enough for small, simple, experimental projects. It doesn't include too much _noise_. It is easy to clean.
    * For more complex projects, it is a repository you can easily extend and grow. Not much needed to clean or adapt.
* It is durable, easy to maintain.
* Similar to FDA 1's initial boilerplate, in terms of features.
* Coherent with all of Fury's initial repositories.

:soon: For more sophisticated or specialized solutions we'll soon be providing other, more opinionated, repository templates.

### Repository features

#### The `src` directory.

A good engineering practice for Python projects is to structure them as [*packages*](https://docs.python.org/3/tutorial/modules.html#packages).

In line with that, a `src` directory has many advantages. If you don't trust us, ask [Hynek](https://hynek.me/articles/testing-packaging/) or [Ionel](https://blog.ionelmc.ro/2014/05/25/python-packaging/#the-structure).

Besides, we've attended many support issues that would have never been if people used the `src` directory.

Finally, `poetry` supports the `src` layout which is great because of what's in the next section.

#### Use Poetry for dependencies declaration

It is MeLi's official [dependencies management tool](https://furydocs.io/code-eco/guide/#/meli_stacks/python/ecosystem?id=dependencies-declaration-with-poetry) for the Python stack.


##### HowTo run your scripts

Let's say you want to run the main ETL that's in `src/app/data/training_dataset.py`.

As Poetry is handling your Python environment, you have two options:

Just run:

```poetry run python src/app/data/training_dataset.py```

Or else, run `poetry shell` to open a shell with the proper environment and then:

```python src/app/data/training_dataset.py```

#### The `app` module

It is a generic name for your main module. You can rename it freely to express your app or model's name. Given the case, you'll need to update the `import` sentences in the code (so better do it sooner than later), and make sure that you also update `tool.poetry.name` in your `pyproyect.toml` file with the same name you gave to the main module.


_DISCLAIMER:_ this is a generic module name because this boilerplate is not (yet) dynamically generated for your app. It is just a copy/paste. In the near future we'll be using a `cookiecutter` template (just like [furycli](https://furydocs.io/docs/guide/#/lang-es/fury/desarrollo?id=_3-configurar-el-ambiente-local-usando-el-furycli)).

#### Python Version

Just like [in FDA 1](https://furydocs.io/docs/guide/#/lang-es/fda-docs/fda/python_versions), to define the python version to work with edit the `.fda-python-version` file.


#### Running the example workflow
The workflow specified in the `workflows.yml` is a simple example to show how workflows work in FDA 2.
In order to prepare and execute the `example_workflow` using the [FDA CLI](https://furydocs.io/molida/guide/#/cli), the following steps should be executed:
1. Install the FDA CLI and configure the environment ([link](https://furydocs.io/molida-cli/guide/#/tutorial?id=instalation-amp-env-configuration)).
2. [Prepare](https://furydocs.io/molida-cli/0.2.5/guide/#/tutorial?id=_2-preparing-a-task-image) the task-images for `create_training_dataset` and `train_example_model`:
   * `fda prepare create_training_dataset --version 1.0.0-running-workflows-example`
   * `fda prepare train_example_model --version 1.0.0-running-workflows-example`
3. Once the task-images have been successfully prepared, prepare the workflow using the command `fda workflow prepare example_workflow --version <some_version>`
4. Once the workflow has been successfully prepared, use the `<plan_id>` to run the workflow using the command `fda workflow run <plan_id>`
