# kafka_emqx_plugin

A Kafka plugin compatible with EMQX 5.X, which uses WebHook to forward event data and message data to Kafka.

## Release

An EMQX plugin release is a tar file including including a subdirectory of this plugin's name and it's version, that contains:

1. A JSON format metadata file describing the plugin
2. Versioned directories for all applications needed for this plugin (source and binaries).

In a shell from this plugin's working directory execute `make rel` to have the package created like:

```
_build/default/emqx_plugrel/emqx_plugin_template-<vsn>.tar.gz
```
## Get Started
1. **Modify Configuration Files**
   - Edit the `rebar.config` file in the EMQX directory to include the `brod` dependency.
   - Update the `emqx/apps/emqx_machine/priv/reboot_lists.eterm` file to include the `brod` dependency.
   - Recompile EMQX after these modifications.

2. **Compile the Plugin**
   - Navigate to the parent directory of the `kafka_emqx_plugin`.
   - Run the following command to compile:
     ```
     make -C kafka_emqx_plugin rel
     ```

3. **Upload and Execute**
   - Upload the compiled file `_build/default/emqx_plugrel/kafka_emqx_plugin-1.0.0.tar.gz` to EMQX.
   - Run the uploaded file on EMQX Dashboard.

See [EMQX documentation](https://docs.emqx.com/en/enterprise/v5.0/extensions/plugins.html) for details on how to deploy custom plugins.
# reference
1. https://github.com/emqx/emqx-plugin-template
2. https://github.com/ULTRAKID/emqx.git
3. https://github.com/emqx/emqx/blob/abeb5e985f48131bc97d8208f9ce7867cacb75af/apps/emqx_machine/priv/reboot_lists.eterm
