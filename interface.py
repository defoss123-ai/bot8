import asyncio
from datetime import datetime
from pathlib import Path

import questionary
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


class UserInterface:
    def __init__(self, pair_manager, trader, logger, context, log_file: str = "operations.log") -> None:
        self.pair_manager = pair_manager
        self.trader = trader
        self.logger = logger
        self.context = context
        self.log_file = Path(log_file)

        self.console = Console()
        self.layout = Layout()
        self.live: Live | None = None
        self.command_queue: asyncio.Queue[str] = asyncio.Queue()

    async def start(self) -> None:
        await self._run_interface()

    async def _run_interface(self) -> None:
        input_task = asyncio.create_task(self._input_handler())

        try:
            with Live(self._generate_layout(), console=self.console, refresh_per_second=1, screen=True) as live:
                self.live = live

                while self.context.running:
                    while not self.command_queue.empty():
                        command = await self.command_queue.get()
                        if command == "menu":
                            await self._show_menu()
                        elif command == "quit":
                            self.context.running = False
                            break

                    live.update(self._generate_layout())
                    await asyncio.sleep(1)
        finally:
            input_task.cancel()
            await asyncio.gather(input_task, return_exceptions=True)

    async def _input_handler(self) -> None:
        while self.context.running:
            cmd = await asyncio.to_thread(input, "\nКоманда ([m]enu / [q]uit): ")
            normalized = cmd.strip().lower()
            if normalized == "m":
                await self.command_queue.put("menu")
            elif normalized == "q":
                await self.command_queue.put("quit")

    def _generate_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(Layout(name="top", ratio=3), Layout(name="bottom", ratio=2))

        table = Table(title=f"Bot Status — {datetime.utcnow().isoformat(timespec='seconds')} UTC")
        table.add_column("Symbol", style="cyan")
        table.add_column("Enabled", justify="center")
        table.add_column("Last Signal", justify="center")
        table.add_column("Position", justify="center")

        active_signals = {}
        if hasattr(self.trader, "active_orders"):
            for order_data in self.trader.active_orders.values():
                symbol = order_data.get("symbol")
                role = order_data.get("role")
                side = order_data.get("side")
                if symbol and role == "entry":
                    active_signals[symbol] = side

        open_positions = self.trader.get_open_positions() if hasattr(self.trader, "get_open_positions") else {}

        for symbol, settings in sorted(self.pair_manager.pairs.items()):
            enabled_text = Text("ON" if settings.get("enabled") else "OFF")
            enabled_text.stylize("green" if settings.get("enabled") else "red")

            signal_text = active_signals.get(symbol, "-")
            signal_style = "yellow" if signal_text in {"LONG", "SHORT"} else "white"
            signal_render = Text(signal_text, style=signal_style)

            pos_data = open_positions.get(symbol)
            if pos_data:
                pos_text = Text(f"{pos_data['side']} ({pos_data['quantity']})", style="magenta")
            else:
                pos_text = Text("-", style="white")

            table.add_row(symbol, enabled_text, signal_render, pos_text)

        logs_panel = Panel(self._render_logs(), title="Последние логи", border_style="blue")

        layout["top"].update(Panel(table, border_style="green"))
        layout["bottom"].update(logs_panel)
        self.layout = layout
        return layout

    def _render_logs(self, limit: int = 10) -> Text:
        if not self.log_file.exists():
            return Text("Лог-файл не найден", style="yellow")

        lines = self.log_file.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]
        rendered = Text()

        for line in lines:
            style = "white"
            upper_line = line.upper()
            if "ERROR" in upper_line or "CRITICAL" in upper_line:
                style = "bold red"
            elif "WARNING" in upper_line:
                style = "yellow"
            elif "INFO" in upper_line:
                style = "green"

            rendered.append(line + "\n", style=style)

        if not lines:
            rendered.append("Логи отсутствуют", style="yellow")

        return rendered

    async def _show_menu(self) -> None:
        if self.live:
            self.live.stop()

        try:
            while self.context.running:
                action = await asyncio.to_thread(
                    lambda: questionary.select(
                        "Выберите действие",
                        choices=[
                            "Добавить пару",
                            "Редактировать пару",
                            "Удалить пару",
                            "Включить пару",
                            "Отключить пару",
                            "Просмотреть открытые позиции",
                            "Выход из меню",
                        ],
                    ).ask()
                )

                if action in (None, "Выход из меню"):
                    break

                if action == "Добавить пару":
                    await self._menu_add_pair()
                elif action == "Редактировать пару":
                    await self._menu_edit_pair()
                elif action == "Удалить пару":
                    await self._menu_remove_pair()
                elif action == "Включить пару":
                    await self._menu_enable_disable_pair(True)
                elif action == "Отключить пару":
                    await self._menu_enable_disable_pair(False)
                elif action == "Просмотреть открытые позиции":
                    self._show_open_positions()
        finally:
            if self.live:
                self.live.start(refresh=True)

    async def _menu_add_pair(self) -> None:
        symbol = await asyncio.to_thread(lambda: questionary.text("Symbol (например BTC/USDT)").ask())
        leverage = await asyncio.to_thread(lambda: questionary.text("Leverage", default="10").ask())
        tp = await asyncio.to_thread(lambda: questionary.text("TP %", default="2.0").ask())
        sl = await asyncio.to_thread(lambda: questionary.text("SL %", default="1.0").ask())
        cancel = await asyncio.to_thread(lambda: questionary.text("Cancel time (sec)", default="60").ask())

        if not symbol:
            return

        await self.pair_manager.add_pair(
            symbol=symbol,
            leverage=int(leverage or 10),
            tp_percent=float(tp or 2.0),
            sl_percent=float(sl or 1.0),
            cancel_time=int(cancel or 60),
            enabled=True,
        )

    async def _menu_edit_pair(self) -> None:
        symbol = await asyncio.to_thread(lambda: questionary.text("Symbol for update").ask())
        if not symbol:
            return

        current = self.pair_manager.get_pair_settings(symbol)
        if not current:
            self.console.print(f"Пара {symbol} не найдена", style="red")
            return

        leverage = await asyncio.to_thread(
            lambda: questionary.text("Leverage", default=str(current["leverage"])).ask()
        )
        tp = await asyncio.to_thread(lambda: questionary.text("TP %", default=str(current["tp_percent"])).ask())
        sl = await asyncio.to_thread(lambda: questionary.text("SL %", default=str(current["sl_percent"])).ask())
        cancel = await asyncio.to_thread(
            lambda: questionary.text("Cancel time (sec)", default=str(current["cancel_time"])).ask()
        )

        await self.pair_manager.update_pair(
            symbol,
            leverage=int(leverage),
            tp_percent=float(tp),
            sl_percent=float(sl),
            cancel_time=int(cancel),
        )

    async def _menu_remove_pair(self) -> None:
        symbol = await asyncio.to_thread(lambda: questionary.text("Symbol to remove").ask())
        if symbol:
            await self.pair_manager.remove_pair(symbol)

    async def _menu_enable_disable_pair(self, enable: bool) -> None:
        symbol = await asyncio.to_thread(lambda: questionary.text("Symbol").ask())
        if not symbol:
            return

        if enable:
            await self.pair_manager.enable_pair(symbol)
        else:
            await self.pair_manager.disable_pair(symbol)

    def _show_open_positions(self) -> None:
        positions = self.trader.get_open_positions()
        positions_table = Table(title="Открытые позиции")
        positions_table.add_column("Symbol", style="cyan")
        positions_table.add_column("Side")
        positions_table.add_column("Entry")
        positions_table.add_column("Qty")

        if not positions:
            positions_table.add_row("-", "-", "-", "-")
        else:
            for symbol, data in positions.items():
                positions_table.add_row(symbol, str(data["side"]), str(data["entry_price"]), str(data["quantity"]))

        self.console.print(positions_table)
