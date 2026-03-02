# ksw AI demo — natural language switching
Output demo/ai.gif

Set Shell "bash"
Set FontSize 14
Set Width 800
Set Height 420
Set Theme "Dracula"
Set Padding 20
Set TypingSpeed 80ms
Set PlaybackSpeed 0.8

Env KUBECONFIG demo/kubeconfig-demo.yaml

Hide
Type "ksw orders-dev"
Enter
Sleep 500ms
Show

Type `ksw ai "switch to payments pdn"`
Sleep 400ms
Enter
Sleep 2500ms

Type `ksw ai "pin payments pdn and orders dev"`
Sleep 400ms
Enter
Sleep 3000ms

Type `ksw ai "go back to the previous one"`
Sleep 400ms
Enter
Sleep 2000ms
