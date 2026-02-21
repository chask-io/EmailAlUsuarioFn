"""
DEVELOPER CODE - IMPLEMENT YOUR BUSINESS LOGIC HERE

EmailAlUsuarioFn - Validates email parameters and forwards email_to_user
events to the orchestrator for actual delivery.

The handler.py file is infrastructure code and should NOT be modified.
"""

import logging
from typing import Any, Dict, List

from chask_foundation.backend.models import OrchestrationEvent

from api.orchestrator_requests import orchestrator_api_manager

logger = logging.getLogger(__name__)


class FunctionBackend:
    """
    Backend para EmailAlUsuarioFn.

    Valida parámetros de correo electrónico (reasoning, body, attachments),
    obtiene los hilos de email asociados a la sesión, normaliza adjuntos,
    y emite un evento email_to_user al orquestador.

    NO usa LLM — es un validador/enrutador simple.
    """

    def __init__(self, orchestration_event: OrchestrationEvent):
        self.orchestration_event = orchestration_event
        logger.info(
            f"Initialized FunctionBackend for org: "
            f"{orchestration_event.organization.organization_id}"
        )

    def process_request(self) -> str:
        """
        Procesa la solicitud de envío de correo electrónico.

        Flujo:
        1. Extrae parámetros del tool call (reasoning, body, attachments)
        2. Obtiene hilos de email de la sesión
        3. Valida y normaliza adjuntos
        4. Emite evento email_to_user al orquestador
        5. Retorna mensaje de éxito (el handler envía function_call_response)

        Returns:
            Mensaje de éxito indicando que el correo fue enviado

        Raises:
            ValueError: Si faltan parámetros requeridos o adjuntos son inválidos
            RuntimeError: Si no se encuentran hilos de email para la sesión
        """
        tool_args = self._extract_tool_args()

        reasoning = tool_args.get("reasoning")
        body = tool_args.get("body")
        attachments = tool_args.get("attachments", [])

        if not reasoning:
            raise ValueError("Falta el parámetro requerido: reasoning")
        if not body:
            raise ValueError("Falta el parámetro requerido: body")

        logger.info(
            f"Processing email request - reasoning: {reasoning[:100]}..., "
            f"body length: {len(body)}, attachments: {len(attachments) if attachments else 0}"
        )

        # Fetch email threads for the session
        channels_resp = orchestrator_api_manager.call(
            "get_orchestration_session_channels",
            orchestration_session_uuid=self.orchestration_event.orchestration_session_uuid,
            access_token=self.orchestration_event.access_token,
            organization_id=self.orchestration_event.organization.organization_id,
        )

        email_threads = channels_resp.get("channels", {}).get("email_threads", [])

        if not email_threads:
            raise RuntimeError(
                "No se encontró ningún canal de tipo 'email_thread' asociado a la sesión."
            )

        selected_thread = email_threads[0]
        thread_idx = 0

        logger.info(f"Selected email thread: {selected_thread.get('subject', 'Unknown')}")

        # Validate and normalize attachments
        processed_attachments = self._process_attachments(attachments)

        # Emit email_to_user event for the orchestrator to handle delivery
        self._send_email_to_user_event(
            reasoning, body, thread_idx, selected_thread, processed_attachments
        )

        success_message = (
            f'Se ha enviado un correo al usuario a través del hilo '
            f'"{selected_thread.get("subject", "Unknown")}" (idx {thread_idx}).'
        )

        logger.info(f"Email processing completed: {success_message}")
        return success_message

    def _extract_tool_args(self) -> Dict[str, Any]:
        """Extract tool call arguments from orchestration event."""
        extra_params = self.orchestration_event.extra_params or {}
        tool_calls = extra_params.get("tool_calls", [])

        if not tool_calls:
            logger.warning("No tool calls found in orchestration event")
            return {}

        tool_call = tool_calls[0]
        return tool_call.get("args", {})

    def _process_attachments(self, attachments: List) -> List[Dict[str, str]]:
        """
        Validate and normalize attachment objects.

        Normalizes uuid/file_uuid and source/origin field variants
        into a consistent {uuid, source} format.

        Args:
            attachments: Raw attachment list from tool call args

        Returns:
            List of normalized attachment dicts

        Raises:
            ValueError: If any attachment has invalid format or missing UUID
        """
        processed = []
        errors = []

        for i, attachment in enumerate(attachments):
            if not isinstance(attachment, dict):
                errors.append(
                    f"Adjunto {i + 1} tiene formato inválido (debe ser un objeto)"
                )
                continue

            uuid_value = attachment.get("uuid") or attachment.get("file_uuid")
            source_value = attachment.get("source") or attachment.get("origin")

            if uuid_value:
                processed.append({
                    "uuid": uuid_value,
                    "source": source_value or "unknown",
                })
                logger.info(
                    f"Processed attachment {i + 1}: uuid={uuid_value}, "
                    f"source={source_value or 'unknown'}"
                )
            else:
                errors.append(
                    f"Adjunto {i + 1} no tiene UUID válido (falta 'uuid' o 'file_uuid')"
                )

        if errors:
            raise ValueError(f"Error procesando adjuntos: {'; '.join(errors)}")

        return processed

    def _send_email_to_user_event(
        self,
        reasoning: str,
        body: str,
        thread_idx: int,
        selected_thread: Dict[str, Any],
        attachments: List[Dict[str, str]],
    ) -> None:
        """
        Emit email_to_user event to the orchestrator for actual email delivery.

        Evolves the parent event via the API to register lineage, then forwards
        the evolved event to Kafka for the orchestrator to process.

        Args:
            reasoning: Explanation of email logic
            body: HTML email content
            thread_idx: Index of the selected email thread
            selected_thread: The selected email thread data
            attachments: Normalized attachment list
        """
        oe = self.orchestration_event

        # Extract tool_call_id for response matching in EmailToUserHandler
        tool_call_id = None
        original_extra = oe.extra_params or {}
        tool_calls = original_extra.get("tool_calls", [])
        if tool_calls:
            tool_call_id = tool_calls[0].get("id")

        extra_params = {
            "body": body,
            "thread_idx": thread_idx,
            "selected_channel": selected_thread,
            "channel_index": thread_idx,
            "attachments": attachments,
            "sender_email": "orchestrator",
            "to": selected_thread.get("customer_email", ""),
            "tool_call_id": tool_call_id,
        }

        logger.info("Evolving event to email_to_user")
        evolve_response = orchestrator_api_manager.call(
            "evolve_event",
            parent_event_uuid=str(oe.event_id),
            event_type="email_to_user",
            source="agent_EmailToUser",
            target="email",
            prompt=reasoning,
            extra_params=extra_params,
            access_token=oe.access_token,
            organization_id=oe.organization.organization_id,
        )

        status_code = evolve_response.get("status_code")
        if status_code and status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to evolve event to email_to_user: {evolve_response.get('error', 'Unknown')}"
            )

        evolved_uuid = evolve_response.get("uuid")
        if not evolved_uuid:
            raise RuntimeError("API response missing uuid for evolved email_to_user event")

        email_event = oe.model_copy(deep=True)
        email_event.event_id = evolved_uuid
        email_event.event_type = "email_to_user"
        email_event.source = "agent_EmailToUser"
        email_event.target = "email"
        email_event.prompt = reasoning
        email_event.extra_params = evolve_response.get("extra_params", extra_params)

        logger.info("Sending email_to_user event to orchestrator via Kafka")
        orchestrator_api_manager.call(
            "forward_oe_to_kafka",
            orchestration_event=email_event.model_dump(),
            topic="orchestrator",
            access_token=email_event.access_token,
            organization_id=email_event.organization.organization_id,
        )
        logger.info(f"email_to_user event sent [evolved from {oe.event_id} -> {evolved_uuid}]")
