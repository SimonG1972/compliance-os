from rich.console import Console
from rich.traceback import install
console = Console()
install(show_locals=False)

def info(msg): console.log(f"[bold cyan]INFO[/] {msg}")
def warn(msg): console.log(f"[bold yellow]WARN[/] {msg}")
def err(msg):  console.log(f"[bold red]ERR[/] {msg}")
