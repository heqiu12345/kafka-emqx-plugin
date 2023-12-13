# kafka_emqx_plugin

Another amazing EMQX plugin.

## Release

An EMQX plugin release is a tar file including including a subdirectory of this plugin's name and it's version, that contains:

1. A JSON format metadata file describing the plugin
2. Versioned directories for all applications needed for this plugin (source and binaries).

In a shell from this plugin's working directory execute `make rel` to have the package created like:

```
_build/default/emqx_plugrel/emqx_plugin_template-<vsn>.tar.gz
```
## Get Started


See [EMQX documentation](https://docs.emqx.com/en/enterprise/v5.0/extensions/plugins.html) for details on how to deploy custom plugins.
# reference
1. https://github.com/emqx/emqx-plugin-template
2. https://github.com/ULTRAKID/emqx.git
3. https://github.com/emqx/emqx/blob/abeb5e985f48131bc97d8208f9ce7867cacb75af/apps/emqx_machine/priv/reboot_lists.eterm
