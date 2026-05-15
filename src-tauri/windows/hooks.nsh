!macro NSIS_HOOK_POSTINSTALL
  DetailPrint "Normalizando permissoes herdadas na pasta do app..."
  nsExec::ExecToLog '"$SYSDIR\icacls.exe" "$INSTDIR" /reset /T /C'
!macroend
