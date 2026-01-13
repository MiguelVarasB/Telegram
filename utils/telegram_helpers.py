import asyncio
from typing import Callable, Any, Optional
from pyrogram.errors import FloodWait


async def handle_floodwait(
    func: Callable,
    *args,
    max_retries: int = 3,
    on_wait: Optional[Callable[[int], None]] = None,
    **kwargs
) -> Any:
    """
    Ejecuta una función asíncrona con manejo automático de FloodWait.
    
    Args:
        func: Función asíncrona a ejecutar
        *args: Argumentos posicionales para la función
        max_retries: Número máximo de reintentos en caso de FloodWait
        on_wait: Callback opcional que se llama con el tiempo de espera (en segundos)
        **kwargs: Argumentos nombrados para la función
    
    Returns:
        El resultado de ejecutar la función
    
    Raises:
        FloodWait: Si se superan los reintentos máximos
        Exception: Cualquier otra excepción lanzada por la función
    """
    retries = 0
    while retries < max_retries:
        try:
            return await func(*args, **kwargs)
        except FloodWait as e:
            retries += 1
            if retries >= max_retries:
                raise
            
            if on_wait:
                on_wait(e.value)
            
            await asyncio.sleep(e.value)
    
    return await func(*args, **kwargs)


async def safe_telegram_operation(
    operation: Callable,
    *args,
    on_floodwait: Optional[Callable[[int], None]] = None,
    on_error: Optional[Callable[[Exception], None]] = None,
    default_return: Any = None,
    **kwargs
) -> Any:
    """
    Ejecuta una operación de Telegram con manejo seguro de errores.
    
    Args:
        operation: Función asíncrona a ejecutar
        *args: Argumentos posicionales
        on_floodwait: Callback cuando ocurre FloodWait (recibe segundos de espera)
        on_error: Callback cuando ocurre otro error (recibe la excepción)
        default_return: Valor a retornar en caso de error
        **kwargs: Argumentos nombrados
    
    Returns:
        Resultado de la operación o default_return en caso de error
    """
    try:
        return await operation(*args, **kwargs)
    except FloodWait as e:
        if on_floodwait:
            on_floodwait(e.value)
        await asyncio.sleep(e.value)
        try:
            return await operation(*args, **kwargs)
        except Exception as retry_error:
            if on_error:
                on_error(retry_error)
            return default_return
    except Exception as e:
        if on_error:
            on_error(e)
        return default_return
