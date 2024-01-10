# Licence

<! --- SPDX-License-Identifier: CC-BY-4.0  -- >

## Troubleshooting

This is a generic chekclist to troubleshoot/diagnose issues that may arrise 
- Identify the issue and try to reproduce it
- All components keep log of actions and issues under /var/log with enough detals to check a specific timestapm and transaction id.  Monitor the logs with with "tail -f /var/log/messgies/argo*" or similar log files and try to reproduce again the  issue.
- Some components may offer a verbose/deubug flag in their configuraiton files if needed follow the neccesary instrcution in configuration to enable it.
